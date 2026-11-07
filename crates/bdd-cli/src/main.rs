//! `krabka-bdd`: lint, example maps, living documentation and the glossary
//! report over Krabka's Gherkin feature files.
//!
//! Exits 0 on success, 1 when a command finds what fails it, and 2 on a
//! usage or IO error.

use {
    clap::{Parser, Subcommand},
    krabka_bdd_cli::{
        Report,
        fs::{EntryKind, FileSystem},
        lint::{self, LintOptions},
    },
    krabka_bdd_model::glossary::Glossary,
    std::{env, fs, process::ExitCode},
};

/// The variable `bazel run` sets to the workspace root.
const WORKSPACE_VAR: &str = "BUILD_WORKSPACE_DIRECTORY";

/// Krabka's Gherkin tool: house-style lint, example maps, living
/// documentation and the glossary report.
///
/// Relative paths are taken from the current directory, or from the
/// workspace root under `bazel run`, and output shows paths relative to it.
#[derive(Debug, Parser)]
#[command(name = "krabka-bdd", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Lint every `.feature` file under the paths against the house style.
    Lint {
        /// A ratchet baseline: fail on a new error and on an entry that no
        /// longer fires, instead of on every error.
        #[arg(long, value_name = "FILE")]
        baseline: Option<String>,
        /// Write the current errors to the baseline file and exit 0.
        #[arg(long, requires = "baseline")]
        write_baseline: bool,
        #[arg(required = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Render each feature as an example map in Markdown.
    Map {
        #[arg(required = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Render a living-documentation index in Markdown, grouped by
    /// capability.
    Docs {
        /// The directory the index is written to; feature links are relative
        /// to it.
        #[arg(long, value_name = "DIR", default_value = ".")]
        out_dir: String,
        #[arg(required = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Report candidate domain terms and unused glossary terms. Advisory:
    /// always exits 0.
    Glossary {
        /// How many steps must use a word before it is a candidate term.
        #[arg(long, value_name = "N", default_value_t = 3)]
        min_steps: usize,
        #[arg(required = true, value_name = "PATH")]
        paths: Vec<String>,
    },
    /// Print the tag table of the Gherkin style guide from the glossary.
    TagsTable,
}

/// The disk, behind the commands' filesystem seam.
struct Disk;

impl FileSystem for Disk {
    fn kind(&self, path: &str) -> Option<EntryKind> {
        let metadata = fs::metadata(path).ok()?;
        if metadata.is_dir() {
            Some(EntryKind::Dir)
        } else {
            Some(EntryKind::File)
        }
    }

    fn list(&self, dir: &str) -> Result<Vec<String>, String> {
        let entries = fs::read_dir(dir).map_err(|error| format!("{dir}: {error}"))?;
        entries
            .map(|entry| {
                entry
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .map_err(|error| format!("{dir}: {error}"))
            })
            .collect()
    }

    fn read(&self, path: &str) -> Result<String, String> {
        fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))
    }

    fn write(&self, path: &str, contents: &str) -> Result<(), String> {
        fs::write(path, contents).map_err(|error| format!("{path}: {error}"))
    }
}

fn run(command: Command, base: &str) -> Result<Report, String> {
    match command {
        Command::Lint {
            baseline,
            write_baseline,
            paths,
        } => {
            let options = LintOptions {
                paths,
                baseline,
                write_baseline,
            };
            lint::run(&Disk, base, Glossary::embedded()?, &options)
        }
        Command::Map { paths } => krabka_bdd_cli::run_map(&Disk, base, &paths),
        Command::Docs { out_dir, paths } => krabka_bdd_cli::run_docs(&Disk, base, &out_dir, &paths),
        Command::Glossary { min_steps, paths } => {
            krabka_bdd_cli::run_glossary(&Disk, base, Glossary::embedded()?, min_steps, &paths)
        }
        Command::TagsTable => krabka_bdd_cli::run_tags_table(),
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let cwd = match env::current_dir() {
        Ok(dir) => dir.to_string_lossy().into_owned(),
        Err(error) => {
            eprintln!("krabka-bdd: cannot read the current directory: {error}");
            return ExitCode::from(2);
        }
    };
    let workspace = env::var(WORKSPACE_VAR).ok();
    let base = krabka_bdd_cli::base_dir(workspace.as_deref(), &cwd);
    match run(cli.command, &base) {
        Ok(report) => {
            print!("{}", report.text);
            if report.failed {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(message) => {
            eprintln!("krabka-bdd: {message}");
            ExitCode::from(2)
        }
    }
}
