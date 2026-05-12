use codex_app_server_protocol::Thread;

use crate::resume_picker::RawReasoningVisibility;
use crate::resume_picker::thread_to_transcript_cells;

use super::RemoteControlForkSource;
use super::RemoteControlSnapshot;

impl RemoteControlSnapshot {
    pub(crate) fn from_app_server_thread(
        thread: &Thread,
        show_raw_agent_reasoning: bool,
        fork_source: Option<RemoteControlForkSource>,
    ) -> Self {
        let raw_reasoning_visibility = if show_raw_agent_reasoning {
            RawReasoningVisibility::Visible
        } else {
            RawReasoningVisibility::Hidden
        };
        let cells = thread_to_transcript_cells(thread, raw_reasoning_visibility);
        RemoteControlSnapshot::from_transcript_cells(
            thread.cwd.display().to_string(),
            fork_source,
            &cells,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use codex_app_server_protocol::CommandExecutionStatus;
    use codex_app_server_protocol::ThreadItem;
    use codex_app_server_protocol::ThreadStatus;
    use codex_app_server_protocol::Turn;
    use codex_app_server_protocol::TurnItemsView;
    use codex_app_server_protocol::TurnStatus;
    use codex_app_server_protocol::UserInput;
    use codex_protocol::ThreadId;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;

    use crate::remote_control::RemoteControlSnapshot;
    use crate::remote_control::RemoteControlTranscriptRole;

    #[test]
    fn maps_app_server_user_and_agent_items_to_phone_transcript_roles() {
        let thread = test_thread(vec![Turn {
            id: "turn-1".to_string(),
            items: vec![
                ThreadItem::UserMessage {
                    id: "user-1".to_string(),
                    content: vec![UserInput::Text {
                        text: "hello from history".to_string(),
                        text_elements: Vec::new(),
                    }],
                },
                ThreadItem::AgentMessage {
                    id: "agent-1".to_string(),
                    text: "hello from codex".to_string(),
                    phase: None,
                    memory_citation: None,
                },
            ],
            items_view: TurnItemsView::Full,
            status: TurnStatus::Completed,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        }]);

        let snapshot = RemoteControlSnapshot::from_app_server_thread(&thread, false, None);

        assert_eq!(snapshot.messages.len(), 2);
        assert_eq!(snapshot.messages[0].role, RemoteControlTranscriptRole::User);
        assert_eq!(snapshot.messages[0].text, "hello from history");
        assert_eq!(
            snapshot.messages[1].role,
            RemoteControlTranscriptRole::Assistant
        );
        assert_eq!(snapshot.messages[1].text, "hello from codex");
    }

    #[test]
    fn maps_app_server_tool_items_to_collapsible_role() {
        let thread = test_thread(vec![Turn {
            id: "turn-1".to_string(),
            items: vec![ThreadItem::CommandExecution {
                id: "cmd-1".to_string(),
                command: "git status --short".to_string(),
                cwd: absolute_cwd(),
                process_id: None,
                source: Default::default(),
                status: CommandExecutionStatus::Completed,
                command_actions: Vec::new(),
                aggregated_output: Some(" M codex-rs/tui/src/remote_control.rs".to_string()),
                exit_code: Some(0),
                duration_ms: None,
            }],
            items_view: TurnItemsView::Full,
            status: TurnStatus::Completed,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        }]);

        let snapshot = RemoteControlSnapshot::from_app_server_thread(&thread, false, None);

        assert_eq!(snapshot.messages.len(), 1);
        assert!(matches!(
            snapshot.messages[0].role,
            RemoteControlTranscriptRole::Tool | RemoteControlTranscriptRole::Status
        ));
        assert!(snapshot.messages[0].text.contains("$ git status --short"));
        assert!(
            snapshot.messages[0]
                .text
                .contains("M codex-rs/tui/src/remote_control.rs")
        );
    }

    #[test]
    fn snapshot_uses_app_server_thread_cwd_and_turn_history() {
        let thread = test_thread(vec![Turn {
            id: "turn-1".to_string(),
            items: vec![ThreadItem::UserMessage {
                id: "user-1".to_string(),
                content: vec![UserInput::Text {
                    text: "existing prompt".to_string(),
                    text_elements: Vec::new(),
                }],
            }],
            items_view: TurnItemsView::Full,
            status: TurnStatus::Completed,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        }]);

        let snapshot = RemoteControlSnapshot::from_app_server_thread(&thread, false, None);

        assert_eq!(snapshot.cwd, thread.cwd.display().to_string());
        assert_eq!(snapshot.messages.len(), 1);
        assert_eq!(snapshot.messages[0].text, "existing prompt");
    }

    fn test_thread(turns: Vec<Turn>) -> codex_app_server_protocol::Thread {
        codex_app_server_protocol::Thread {
            id: ThreadId::new().to_string(),
            session_id: ThreadId::new().to_string(),
            forked_from_id: None,
            preview: String::new(),
            ephemeral: false,
            model_provider: "openai".to_string(),
            created_at: 0,
            updated_at: 0,
            status: ThreadStatus::Idle,
            path: None,
            cwd: absolute_cwd(),
            cli_version: "test".to_string(),
            source: codex_protocol::protocol::SessionSource::Cli.into(),
            thread_source: None,
            agent_nickname: None,
            agent_role: None,
            git_info: None,
            name: None,
            turns,
        }
    }

    fn absolute_cwd() -> AbsolutePathBuf {
        AbsolutePathBuf::try_from(std::env::current_dir().expect("cwd"))
            .expect("cwd should be absolute")
    }
}
