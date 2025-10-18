use serde_json::json;
use serde_json::to_string_pretty;

use anyhow::Result;
use chrono::prelude::*;
use clap::ArgMatches;
use std::path::Path;
use std::sync::Arc;

use crate::config;
use crate::output::Output;
use crate::paths::*;

//-----------------------------------------

fn fmt_time(t: &chrono::DateTime<FixedOffset>) -> String {
    t.format("%b %d %y %H:%M").to_string()
}

pub fn run(matches: &ArgMatches, output: Arc<Output>) -> Result<()> {
    let archive_dir = Path::new(matches.get_one::<String>("ARCHIVE").unwrap()).canonicalize()?;
    let mut streams = Vec::new();

    for stream_dir in stream_iter_lazy(&archive_dir)? {
        let id = stream_dir
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        let cfg = config::read_stream_config(&archive_dir, &id)?;
        streams.push((id, config::to_date_time(&cfg.pack_time), cfg));
    }

    streams.sort_by(|l, r| l.1.cmp(&r.1));

    if output.json {
        let mut j_output = Vec::new();
        for (id, time, cfg) in streams {
            let source = cfg.name.unwrap();
            let size = cfg.size;
            j_output.push(json!(
                {"stream_id": id, "size": size, "time": time.to_rfc3339(), "source": source, "input_file": cfg.source_path}
            ));
        }

        println!("{}", to_string_pretty(&j_output).unwrap());
    } else {
        // calc size width
        let mut width = 0;
        for (_, _, cfg) in &streams {
            let txt = format!("{}", cfg.size);
            if txt.len() > width {
                width = txt.len();
            }
        }

        for (id, time, cfg) in streams {
            let source = cfg.name.unwrap();
            let size = cfg.size;
            output.report.to_stdout(&format!(
                "{} {:width$} {} {}",
                id,
                size,
                &fmt_time(&time),
                &source
            ));
        }
    }
    Ok(())
}

//-----------------------------------------
