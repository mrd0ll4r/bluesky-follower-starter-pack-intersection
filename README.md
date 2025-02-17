# Bluesky Follower Starter Pack Intersection

A tool to intersect Bluesky multi-follow commits with starter pack member lists, at high speeds.
This uses all available cores to calculate the overlaps between two lists of sets.

Used in the paper **Bootstrapping Social Networks: Lessons from Bluesky Starter Packs**.

If you use this tool, or it was otherwise useful to you, please cite:
```
@misc{balduf2025bootstrappingsocialnetworks,
    title={Bootstrapping Social Networks: Lessons from Bluesky Starter Packs},
    author={Leonhard Balduf and Saidu Sokoto and Onur Ascigil and Gareth Tyson and Ignacio Castro and Andrea Baronchelli and George Pavlou and Björn Scheuermann and Michał Król},
    year={2025},
    eprint={2501.11605},
    archivePrefix={arXiv},
    primaryClass={cs.SI},
    url={https://arxiv.org/abs/2501.11605},
}
```

## Build

We want maximal performance.
This uses `rayon` for parallelism.
Build as such:
```
cargo build --locked --release
```

## Running

```
Intersects CSVs of multi-follow operations and starter pack members

Usage: follower_starter_pack_intersection --multi <PATH> --lists <PATH> --date <DATE>

Options:
      --multi <PATH>  path of the file containing multi-follow operations of one day.
      --lists <PATH>  path of a file containing membership changes in starter pack lists, ordered by time.
      --date <DATE>   the date on which to operate. This is used to replay membership changes up to the date of the multi-follow operations.
  -h, --help          Print help
  -V, --version       Print version
```

You'll need multiple input files:

1. A file of multi-follow operations per date.
    This should be gzipped CSV of the format
    ```
    did,seq,commit_ts,follow_created_at,follow_subject
    ```
    Where the `seq` field is used to group by.
    The `follow_subject` field specify the subjects of the follow operation. 
    For example:
    ```
    did,seq,commit_ts,follow_created_at,follow_subject
    did:plc:foo,1,2024-11-20T09:17:48Z,2024-11-20T09:17:48Z,did:plc:somedid1
    did:plc:foo,1,2024-11-20T09:17:48Z,2024-11-20T09:17:48Z,did:plc:somedid2
    did:plc:foo,1,2024-11-20T09:17:48Z,2024-11-20T09:17:48Z,did:plc:somedid3
    did:plc:foo,1,2024-11-20T09:17:48Z,2024-11-20T09:17:48Z,did:plc:somedid4
    did:plc:bar,2,2024-11-20T09:17:52Z,2024-11-20T09:17:52Z,did:plc:somedid5
    did:plc:bar,2,2024-11-20T09:17:52Z,2024-11-20T09:17:52Z,did:plc:somedid6
    did:plc:bar,2,2024-11-20T09:17:52Z,2024-11-20T09:17:52Z,did:plc:somedid7
    did:plc:bar,2,2024-11-20T09:17:52Z,2024-11-20T09:17:52Z,did:plc:somedid8
    did:plc:bar,2,2024-11-20T09:17:52Z,2024-11-20T09:17:52Z,did:plc:somedid9
    ```
2. A file that lists changes to starter pack memberships, ordered by timestamp.
    This should be gzipped CSV of the format
    ```
    time,operation,user,uri
    ```
    For example:
    ```
    time,operation,user,uri
    2024-06-10T08:11:39.781Z,c,did:plc:somedid,at://did:plc:foo/app.bsky.graph.list/3kkuqllisjd2o
    2024-06-10T08:11:57.061Z,c,did:plc:somedid,at://did:plc:foo/app.bsky.graph.list/3knatsbh35k2b
    2024-06-10T08:11:58.293Z,c,did:plc:somedid,at://did:plc:foo/app.bsky.graph.list/3kn7onfsxec2q
    2024-06-10T08:12:59.651Z,c,did:plc:somedid,at://did:plc:bar/app.bsky.graph.list/3kukmozp7hg2h
    2024-06-10T08:13:04.355Z,c,did:plc:somedid,at://did:plc:bar/app.bsky.graph.list/3kuewqihfe22o
    2024-06-10T08:13:09.023Z,c,did:plc:somedid,at://did:plc:bar/app.bsky.graph.list/3kukmvrgfx52m
    2024-06-10T08:13:12.872Z,c,did:plc:somedid,at://did:plc:baz/app.bsky.graph.list/3kchkt2inol2q
    2024-06-10T08:13:30.969Z,c,did:plc:somedid,at://did:plc:baz/app.bsky.graph.list/3kukmw2sjdc2e
    2024-06-10T08:13:33.991Z,d,did:plc:somedid,at://did:plc:baz/app.bsky.graph.list/3kukmw2sjdc2e
    ```
   
You specify the two above files and the date of the multi-follow file.
The state of all starter packs will be recreated by replaying the membership operations.
Then, for each `seq`,`uri` pair, an overlap score will be calculated.

Output is printed to stdout in this format:
```csv
seq,uri,multi_follow_size,starter_pack_size,intersection_size,size_diff_factor,overlap,result
```
where `seq` and `uri` identify the multi-follow operation and starter pack list, respectively.

Most of the other fields are for debugging.
The `result` is a value in `[0,1]` which indicates how well the two match.
The top ten matches per pair are returned.