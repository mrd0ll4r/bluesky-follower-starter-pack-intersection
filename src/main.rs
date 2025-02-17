use anyhow::{bail, Context, Result};
use chrono::{DateTime, NaiveDate};
use clap::{arg, command};
use libflate::gzip;
use log::{debug, info, warn};
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

fn main() -> Result<()> {
    flexi_logger::init();

    let matches = command!()
        .about("Intersects CSVs of multi-follow operations and starter pack members")
        .arg(arg!(--multi <PATH> "path of the file containing multi-follow operations of one day.").required(true))
        .arg(arg!(--lists <PATH> "path of a file containing membership changes in starter pack lists, ordered by time.").required(true))
        .arg(arg!(--date <DATE> "the date on which to operate. This is used to replay membership changes up to the date of the multi-follow operations.").required(true))
        .get_matches();

    let multi_follow_file_path = matches.get_one::<String>("multi").expect("required");
    let list_membership_file_path = matches.get_one::<String>("lists").expect("required");
    let date = matches.get_one::<String>("date").expect("required");
    let date = NaiveDate::parse_from_str(&date, "%Y-%m-%d").context("unable to parse date")?;

    debug!(
        "reading list membership from {}...",
        list_membership_file_path
    );
    let starter_pack_participants = read_lists_file(list_membership_file_path, date)
        .context("unable to read starter pack member lists")?;
    info!(
        "read {} starter packs with participants until and including {}",
        starter_pack_participants.len(),
        date
    );

    debug!(
        "reading multi-follows file from {}..",
        multi_follow_file_path
    );
    let multi_follows = read_multi_follows_file(multi_follow_file_path)
        .context("unable to read multi-follows file")?;
    info!("read {} multi-follows", multi_follows.len());

    // Print header
    println!("seq,uri,multi_follow_size,starter_pack_size,intersection_size,size_diff_factor,overlap,result");

    multi_follows
        .into_iter()
        //.take(10000)
        .collect::<Vec<_>>()
        .par_iter()
        .flat_map_iter(|(seq, followers)| {
            let mut matches = find_best_matches(followers, &starter_pack_participants);
            matches.sort_unstable_by(|a,b| {
                a.result.partial_cmp(&b.result).unwrap_or(Ordering::Equal)
            });
            matches.reverse();

            debug!(
                "matches first element result is {:?}, last element result is {:?}, best result is {:?}",
                matches.first(),
                matches.last(),
                matches.iter().max_by(|a,b| a.result.partial_cmp(&b.result).unwrap_or(Ordering::Equal))
            );
            matches
                .into_iter()
                //.filter(|elem| elem.result >= OVERLAP_CUTOFF)
                .take(10)
                .map(|elem| {
                (
                    *seq,
                    elem.uri,
                    elem.multi_follow_size,
                    elem.starter_pack_size,
                    elem.intersection_size,
                    elem.size_diff_factor,
                    elem.overlap,
                    elem.result,
                )
            })
        })
        .for_each(
            |(
                seq,
                uri,
                multi_follow_size,
                starter_pack_size,
                intersection_size,
                size_diff_factor,
                overlap,
                result,
            )| {
                println!(
                    "{},{},{},{},{},{},{},{}",
                    seq,
                    uri,
                    multi_follow_size,
                    starter_pack_size,
                    intersection_size,
                    size_diff_factor,
                    overlap,
                    result
                );
            },
        );

    Ok(())
}

fn read_multi_follows_file<P: AsRef<Path>>(path: P) -> Result<Vec<(i64, HashSet<String>)>> {
    let reader = gzip::Decoder::new(BufReader::new(
        File::open(path).context("unable to open file")?,
    ))
    .context("unable to parse GZIP")?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(',' as u8)
        .from_reader(reader);
    let mut to_return = HashMap::new();

    for result in reader.records() {
        let record = result.context("unable to parse CSV")?;
        let seq = record
            .get(1)
            .context("missing seq field")?
            .to_string()
            .parse::<i64>()
            .context("unable to parse seq as i64")?;
        let followee = record.get(4).context("missing followee field")?.to_string();

        to_return
            .entry(seq)
            .or_insert_with(HashSet::new)
            .insert(followee);
    }

    Ok(to_return.into_iter().collect())
}

fn read_lists_file<P: AsRef<Path>>(
    path: P,
    up_to_date: NaiveDate,
) -> Result<Vec<(String, HashSet<String>)>> {
    let reader = gzip::Decoder::new(BufReader::new(
        File::open(path).context("unable to open file")?,
    ))
    .context("unable to parse GZIP")?;
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(',' as u8)
        .from_reader(reader);
    let mut to_return = HashMap::new();

    for result in reader.records() {
        let record = result.context("unable to parse CSV")?;
        let date = record.get(0).context("missing date field")?.to_string();
        let date = DateTime::parse_from_rfc3339(&date)
            .context("unable to parse date")?
            .date_naive();
        if date > up_to_date {
            // We assume the list is sorted
            break;
        }
        let operation = record
            .get(1)
            .context("missing operation field")?
            .to_string();
        let did = record.get(2).context("missing user field")?.to_string();
        let uri = record.get(3).context("missing uri field")?.to_string();

        let list = to_return.entry(uri.clone()).or_insert_with(HashSet::new);
        match operation.as_str() {
            "c" => {
                list.insert(did);
            }
            "d" => {
                if !list.remove(&did) {
                    warn!("removal of {} from list {} but was not present", did, uri)
                }
            }
            _ => {
                bail!("invalid operation: {}", operation);
            }
        }
    }

    Ok(to_return
        .into_iter()
        .filter(|(_, members)| !members.is_empty())
        .collect())
}

struct StarterPack<'a> {
    uri: &'a str,
    participants: &'a HashSet<String>,
}

impl<'a> StarterPack<'a> {
    fn calculate_overlap(self, to: &HashSet<String>) -> StarterPackOverlap<'a> {
        let intersection_size = self.participants.intersection(to).count();
        let larger_set_size = to.len().max(self.participants.len());
        let size_difference = intersection_size.abs_diff(larger_set_size);
        let size_diff_factor = 1_f64 - (size_difference as f64 / larger_set_size as f64);
        let overlap = (intersection_size as f64) / (to.len().min(self.participants.len()) as f64);
        StarterPackOverlap {
            uri: self.uri,
            multi_follow_size: to.len(),
            starter_pack_size: self.participants.len(),
            intersection_size,
            size_diff_factor,
            overlap,
            result: overlap * size_diff_factor,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct StarterPackOverlap<'a> {
    uri: &'a str,
    multi_follow_size: usize,
    starter_pack_size: usize,
    intersection_size: usize,
    overlap: f64,
    size_diff_factor: f64,
    result: f64,
}

impl Eq for StarterPackOverlap<'_> {}
impl Ord for StarterPackOverlap<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.result.partial_cmp(&other.result).unwrap()
    }
}

fn find_best_matches<'a>(
    follow_members: &HashSet<String>,
    b: &'a [(String, HashSet<String>)],
) -> Vec<StarterPackOverlap<'a>> {
    b.iter()
        .map(|(uri, participants)| StarterPack {
            uri: uri.as_str(),
            participants,
        })
        .map(|sp| sp.calculate_overlap(follow_members))
        .filter(|sp| sp.overlap > 0f64)
        .collect()
}
