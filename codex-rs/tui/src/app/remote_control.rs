use super::*;
use crate::remote_control::RemoteControlForkSource;
use crate::remote_control::RemoteControlForkStatus;
use crate::remote_control::RemoteControlSnapshot;
use crate::remote_control::RemoteControlTranscriptItem;
use crate::remote_control::RemoteControlTranscriptRole;

const REMOTE_CONTROL_TRANSCRIPT_WIDTH: u16 = 88;

impl App {
    pub(super) fn start_local_remote_control(
        &mut self,
        options: crate::remote_control::LocalRemoteControlOptions,
    ) {
        if self.local_remote_control_server.is_some() {
            self.show_local_remote_control_pairing();
            return;
        }

        let snapshot = self.remote_control_snapshot();
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
            Line::from("Scan the QR code with your phone to continue this Codex session."),
            Line::from(""),
        ];
        lines.extend(server.qr_lines());
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            "URL: ".dim(),
            server.url().to_string().cyan(),
        ]));
        lines.push(
            "Anyone with this URL can submit prompts to this local Codex session."
                .dim()
                .into(),
        );
        self.chat_widget.add_plain_history_lines(lines);
    }

    fn remote_control_snapshot(&self) -> RemoteControlSnapshot {
        let mut messages = Vec::new();
        for cell in &self.transcript_cells {
            let role = role_for_cell(cell.as_ref());
            let text = text_for_cell(cell.as_ref());
            push_remote_control_text(&mut messages, role, text);
        }
        if let Some(lines) = self
            .chat_widget
            .active_cell_transcript_lines(REMOTE_CONTROL_TRANSCRIPT_WIDTH)
        {
            push_remote_control_text(
                &mut messages,
                RemoteControlTranscriptRole::Assistant,
                lines_to_text(lines),
            );
        }
        let fork_source = self.remote_control_fork_source();
        let fork = RemoteControlForkStatus {
            available: fork_source.is_some(),
            thread_id: fork_source.as_ref().map(|source| source.thread_id.clone()),
        };

        RemoteControlSnapshot {
            cwd: self.config.cwd.display().to_string(),
            status: "Connected to Codex".to_string(),
            messages: messages
                .into_iter()
                .enumerate()
                .map(|(index, (role, text))| RemoteControlTranscriptItem {
                    id: index.saturating_add(1),
                    role,
                    text,
                })
                .collect(),
            fork,
            fork_source,
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

fn role_for_cell(cell: &dyn HistoryCell) -> RemoteControlTranscriptRole {
    let any = cell.as_any();
    if any.is::<history_cell::UserHistoryCell>() {
        RemoteControlTranscriptRole::User
    } else if any.is::<history_cell::AgentMessageCell>()
        || any.is::<history_cell::AgentMarkdownCell>()
        || any.is::<history_cell::ReasoningSummaryCell>()
        || any.is::<history_cell::ProposedPlanCell>()
        || any.is::<history_cell::ProposedPlanStreamCell>()
        || any.is::<history_cell::PlanUpdateCell>()
    {
        RemoteControlTranscriptRole::Assistant
    } else if any.is::<history_cell::PlainHistoryCell>()
        || any.is::<history_cell::SessionHeaderHistoryCell>()
    {
        RemoteControlTranscriptRole::Status
    } else {
        RemoteControlTranscriptRole::Tool
    }
}

fn text_for_cell(cell: &dyn HistoryCell) -> String {
    let raw_lines = cell.raw_lines();
    if raw_lines.is_empty() {
        lines_to_text(cell.transcript_lines(REMOTE_CONTROL_TRANSCRIPT_WIDTH))
    } else {
        lines_to_text(raw_lines)
    }
}

fn push_remote_control_text(
    messages: &mut Vec<(RemoteControlTranscriptRole, String)>,
    role: RemoteControlTranscriptRole,
    text: String,
) {
    let text = text.trim().to_string();
    if text.is_empty() {
        return;
    }

    if let Some((last_role, last_text)) = messages.last_mut()
        && *last_role == role
    {
        last_text.push_str("\n\n");
        last_text.push_str(&text);
        return;
    }

    messages.push((role, text));
}

fn lines_to_text(lines: Vec<Line<'static>>) -> String {
    lines
        .into_iter()
        .map(|line| {
            line.spans
                .into_iter()
                .map(|span| span.content.into_owned())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}
