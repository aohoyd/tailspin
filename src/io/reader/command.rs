use crate::io::reader::buffer_line_counter::{BUFF_READER_CAPACITY, ReadResult, read_lines};
use crate::io::reader::{AsyncLineReader, StreamEvent};
use async_trait::async_trait;
use miette::{Context, IntoDiagnostic, Result, miette};
use std::process::Stdio;
use tokio::io::BufReader;
use tokio::process::{Child, ChildStdout, Command};

#[cfg(unix)]
use std::process::Command as StdCommand;

pub struct CommandReader {
    reader: BufReader<ChildStdout>,
    child: Child,
    ready: bool,
}

impl CommandReader {
    pub async fn new(command: String) -> Result<CommandReader> {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg(command).stdout(Stdio::piped());

        #[cfg(unix)]
        cmd.process_group(0);

        let mut child = cmd.spawn().into_diagnostic().wrap_err("Could not spawn process")?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| miette!("Could not capture stdout of spawned process"))?;

        let reader = BufReader::with_capacity(BUFF_READER_CAPACITY, stdout);

        Ok(CommandReader {
            reader,
            child,
            ready: false,
        })
    }

    #[cfg(unix)]
    fn kill_process_group(&self) {
        if let Some(pid) = self.child.id() {
            // Kill the entire process group using the kill command
            // Negative PID means process group (PGID = PID since we used process_group(0))
            let _ = StdCommand::new("kill").arg("--").arg(format!("-{}", pid)).status();
        }
    }

    #[cfg(not(unix))]
    fn kill_process_group(&self) {
        // On non-Unix systems, we rely on the child.kill() fallback
    }
}

impl Drop for CommandReader {
    fn drop(&mut self) {
        self.kill_process_group();
    }
}

#[async_trait]
impl AsyncLineReader for CommandReader {
    async fn next(&mut self) -> Result<StreamEvent> {
        if !self.ready {
            self.ready = !self.ready;

            return Ok(StreamEvent::Started);
        }

        read_lines(&mut self.reader).await.map(|res| match res {
            ReadResult::Eof => StreamEvent::Ended,
            ReadResult::Line(line) => StreamEvent::Line(line),
            ReadResult::Batch(lines) => StreamEvent::Lines(lines),
        })
    }
}
