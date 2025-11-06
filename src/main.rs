use anyhow::{anyhow, Result};
use std::process::exit;
use std::sync::Arc;

use blk_stash::create;
use blk_stash::dump_stream;
use blk_stash::list;
use blk_stash::output::Output;
use blk_stash::pack;
use blk_stash::recovery::confirm_data_loss_warning;
use blk_stash::recovery::flight_check;
use blk_stash::unpack;
use blk_stash::utils::mk_output;

//-----------------------

fn with_output<F>(matches: &clap::ArgMatches, f: F) -> Result<()>
where
    F: FnOnce(Arc<Output>) -> Result<()>,
{
    let json = matches.get_flag("JSON");
    let output = mk_output(json);
    f(output)
}

fn main_() -> Result<()> {
    let cli = cli::build_cli();
    let matches = cli.get_matches();

    // Check for and apply recovery checkpoint before processing any command
    // This handles interrupted operations by truncating files to last known-good state
    // and cleaning up indexs, cuckoo filters etc.
    if let Some((subcommand_name, sub_matches)) = matches.subcommand() {
        if let Some(archive_path) = sub_matches.get_one::<String>("ARCHIVE") {
            // Do a preflight check before proceeding to ensure the archive is in a hopefully
            // good state (skip for create command as archive doesn't exist yet)
            if subcommand_name != "create" && std::env::var("BLK_STASH_DEVEL_SKIP_DATA").is_err() {
                let fc = flight_check(archive_path);
                if fc.is_err() {
                    if subcommand_name == "verify" && sub_matches.get_flag("REPAIR") {
                        if !confirm_data_loss_warning() {
                            return Err(anyhow!(
                                "User selected to NOT confirm data loss, exiting!"
                            ));
                        }
                    } else {
                        eprintln!("Archive is in an inconsistent state, try verify --all --repair");
                        return fc;
                    }
                }
            }
        }
    }

    match matches.subcommand() {
        Some(("create", sub_matches)) => {
            let output = mk_output(false);
            create::run(sub_matches, output.report.clone())?;
        }
        Some(("pack", sub)) => {
            with_output(sub, |out| pack::run(sub, out))?;
        }
        Some(("unpack", sub)) => {
            with_output(sub, |out| unpack::run_unpack(sub, out))?;
        }
        Some(("verify", sub)) => {
            with_output(sub, |out| unpack::run_verify(sub, out))?;
        }
        Some(("list", sub)) => {
            with_output(sub, |out| list::run(sub, out))?;
        }
        Some(("dump-stream", sub)) => {
            with_output(sub, |out| dump_stream::run(sub, out))?;
        }
        _ => unreachable!("Exhausted list of subcommands and subcommand_required prevents 'None'"),
    }

    Ok(())
}

fn main() {
    let code = match main_() {
        Ok(()) => 0,
        Err(e) => {
            // FIXME: write to report
            eprintln!("{e:?}");
            // We don't print out the error since -q may be set
            1
        }
    };

    exit(code)
}

//-----------------------
