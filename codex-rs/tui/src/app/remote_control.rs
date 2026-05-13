use super::*;
use crate::remote_control::REMOTE_CONTROL_TRANSCRIPT_WIDTH;
use crate::remote_control::RemoteControlForkSource;
use crate::remote_control::RemoteControlSnapshot;

impl App {
    pub(super) async fn start_local_remote_control(
        &mut self,
        app_server: &mut AppServerSession,
        options: crate::remote_control::LocalRemoteControlOptions,
    ) {
        if self.local_remote_control_server.is_some() {
            self.show_local_remote_control_pairing();
            return;
        }

        let snapshot = self
            .remote_control_snapshot_from_app_server(app_server)
            .await
            .unwrap_or_else(|| self.remote_control_snapshot());
        match crate::remote_control::start_local_server(
            options.clone(),
            self.app_event_tx.clone(),
            snapshot,
        ) {
            Ok(server) => {
                self.local_remote_control_options = options;
                self.local_remote_control_server = Some(server);
                self.show_local_remote_control_pairing();
            }
            Err(err) => {
                self.chat_widget
                    .add_error_message(format!("Remote control failed to start: {err}"));
            }
        }
    }

    pub(super) fn stop_local_remote_control(&mut self) {
        if self.local_remote_control_server.take().is_some() {
            self.chat_widget
                .add_info_message("Remote control stopped.".to_string(), /*hint*/ None);
        } else {
            self.chat_widget.add_info_message(
                "Remote control is not running.".to_string(),
                Some("Use /remote-control to start it.".to_string()),
            );
        }
    }

    pub(super) fn sync_remote_control_snapshot(&self) {
        if let Some(server) = &self.local_remote_control_server {
            server.update_snapshot(self.remote_control_snapshot());
        }
    }

    fn show_local_remote_control_pairing(&mut self) {
        let Some(server) = &self.local_remote_control_server else {
            return;
        };
        let mut lines = vec![
            Line::from(vec!["Remote control ".bold(), "active".green()]),
            Line::from("Scan the QR code with your phone to control this Codex session."),
            Line::from(""),
        ];
        lines.extend(server.qr_lines());
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            "Controller link: ".dim(),
            server.url().to_string().cyan(),
        ]));
        lines.push(
            "The controller link can submit prompts to this local Codex session."
                .dim()
                .into(),
        );
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            "Share link: ".dim(),
            server.share_url().to_string().cyan(),
        ]));
        lines.push(
            "The share link is read-only: viewers can watch live and fork, but cannot submit prompts."
                .dim()
                .into(),
        );
        self.chat_widget.add_plain_history_lines(lines);
    }

    fn remote_control_snapshot(&self) -> RemoteControlSnapshot {
        RemoteControlSnapshot::from_transcript_cells(
            self.config.cwd.display().to_string(),
            self.remote_control_fork_source(),
            &self.transcript_cells,
            self.chat_widget
                .active_cell_transcript_lines(REMOTE_CONTROL_TRANSCRIPT_WIDTH),
        )
    }

    async fn remote_control_snapshot_from_app_server(
        &mut self,
        app_server: &mut AppServerSession,
    ) -> Option<RemoteControlSnapshot> {
        let thread_id = self.current_displayed_thread_id()?;
        match app_server
            .thread_read(thread_id, /*include_turns*/ true)
            .await
        {
            Ok(thread) => Some(RemoteControlSnapshot::from_app_server_thread(
                &thread,
                self.config.show_raw_agent_reasoning,
                self.remote_control_fork_source(),
            )),
            Err(err) => {
                tracing::warn!(
                    %thread_id,
                    "failed to read app-server thread for remote-control snapshot: {err:#}"
                );
                None
            }
        }
    }

    fn remote_control_fork_source(&self) -> Option<RemoteControlForkSource> {
        let session = self.primary_session_configured.as_ref()?;
        let rollout_path = session.rollout_path.clone()?;
        if !rollout_path.is_file() {
            return None;
        }
        Some(RemoteControlForkSource {
            thread_id: session.thread_id.to_string(),
            cwd: session.cwd.display().to_string(),
            rollout_path,
        })
    }
}
