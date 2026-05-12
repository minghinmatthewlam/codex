use codex_app_server_protocol::CommandExecutionStatus;
use codex_app_server_protocol::Thread;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::Turn;
use codex_app_server_protocol::UserInput;

use super::RemoteControlForkSource;
use super::RemoteControlForkStatus;
use super::RemoteControlSnapshot;
use super::RemoteControlTranscriptItem;
use super::RemoteControlTranscriptRole;

impl RemoteControlSnapshot {
    pub(crate) fn from_app_server_thread(
        thread: &Thread,
        fork_source: Option<RemoteControlForkSource>,
    ) -> Self {
        let mut messages = Vec::new();
        for turn in &thread.turns {
            push_turn_messages(&mut messages, turn);
        }
        let fork = RemoteControlForkStatus {
            available: fork_source.is_some(),
            thread_id: fork_source.as_ref().map(|source| source.thread_id.clone()),
        };
        Self {
            cwd: thread.cwd.display().to_string(),
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
}

fn push_turn_messages(messages: &mut Vec<(RemoteControlTranscriptRole, String)>, turn: &Turn) {
    for item in &turn.items {
        if let Some((role, text)) = message_from_thread_item(item) {
            push_message(messages, role, text);
        }
    }
}

fn message_from_thread_item(item: &ThreadItem) -> Option<(RemoteControlTranscriptRole, String)> {
    match item {
        ThreadItem::UserMessage { content, .. } => {
            Some((RemoteControlTranscriptRole::User, user_input_text(content)))
        }
        ThreadItem::HookPrompt { fragments, .. } => Some((
            RemoteControlTranscriptRole::Status,
            fragments
                .iter()
                .map(|fragment| fragment.text.trim())
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n"),
        )),
        ThreadItem::AgentMessage { text, .. } | ThreadItem::Plan { text, .. } => {
            Some((RemoteControlTranscriptRole::Assistant, text.clone()))
        }
        ThreadItem::Reasoning {
            summary, content, ..
        } => {
            let text = summary
                .iter()
                .chain(content.iter())
                .map(|text| text.trim())
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            Some((RemoteControlTranscriptRole::Assistant, text))
        }
        ThreadItem::CommandExecution {
            command,
            status,
            aggregated_output,
            exit_code,
            ..
        } => {
            let mut text = format!("$ {command}");
            if let Some(output) = aggregated_output
                .as_ref()
                .filter(|output| !output.is_empty())
            {
                text.push('\n');
                text.push_str(output);
            }
            if !matches!(
                status,
                CommandExecutionStatus::Completed | CommandExecutionStatus::InProgress
            ) && let Some(exit_code) = exit_code
            {
                text.push_str(&format!("\nexit code: {exit_code}"));
            }
            Some((RemoteControlTranscriptRole::Tool, text))
        }
        ThreadItem::FileChange {
            changes, status, ..
        } => {
            let paths = changes
                .iter()
                .map(|change| change.path.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            Some((
                RemoteControlTranscriptRole::Tool,
                format!("File changes {status:?}: {paths}"),
            ))
        }
        ThreadItem::McpToolCall {
            server,
            tool,
            status,
            result,
            error,
            ..
        } => {
            let mut text = format!("MCP {server}.{tool} {status:?}");
            if let Some(error) = error {
                text.push_str(&format!("\n{error:?}"));
            } else if let Some(result) = result {
                text.push_str(&format!("\n{result:?}"));
            }
            Some((RemoteControlTranscriptRole::Tool, text))
        }
        ThreadItem::DynamicToolCall {
            namespace,
            tool,
            status,
            success,
            ..
        } => {
            let name = namespace
                .as_ref()
                .map(|namespace| format!("{namespace}.{tool}"))
                .unwrap_or_else(|| tool.clone());
            Some((
                RemoteControlTranscriptRole::Tool,
                format!("Tool {name} {status:?} success={success:?}"),
            ))
        }
        ThreadItem::CollabAgentToolCall {
            tool,
            status,
            prompt,
            ..
        } => {
            let mut text = format!("Agent tool {tool:?} {status:?}");
            if let Some(prompt) = prompt.as_ref().filter(|prompt| !prompt.is_empty()) {
                text.push_str("\n\n");
                text.push_str(prompt);
            }
            Some((RemoteControlTranscriptRole::Tool, text))
        }
        ThreadItem::WebSearch { query, action, .. } => Some((
            RemoteControlTranscriptRole::Tool,
            format!("Web search: {query}\n{action:?}"),
        )),
        ThreadItem::ImageView { path, .. } => Some((
            RemoteControlTranscriptRole::Tool,
            format!("Viewed image: {}", path.display()),
        )),
        ThreadItem::ImageGeneration {
            status,
            revised_prompt,
            result,
            saved_path,
            ..
        } => {
            let mut text = format!("Image generation {status}: {result}");
            if let Some(prompt) = revised_prompt.as_ref().filter(|prompt| !prompt.is_empty()) {
                text.push_str("\n\n");
                text.push_str(prompt);
            }
            if let Some(path) = saved_path {
                text.push_str(&format!("\nSaved to {}", path.display()));
            }
            Some((RemoteControlTranscriptRole::Tool, text))
        }
        ThreadItem::EnteredReviewMode { review, .. } => Some((
            RemoteControlTranscriptRole::Status,
            format!("Entered review mode: {review}"),
        )),
        ThreadItem::ExitedReviewMode { review, .. } => Some((
            RemoteControlTranscriptRole::Status,
            format!("Exited review mode: {review}"),
        )),
        ThreadItem::ContextCompaction { .. } => Some((
            RemoteControlTranscriptRole::Status,
            "Context compacted".to_string(),
        )),
    }
}

fn user_input_text(content: &[UserInput]) -> String {
    content
        .iter()
        .map(|input| match input {
            UserInput::Text { text, .. } => text.clone(),
            UserInput::Image { .. } => "[image]".to_string(),
            UserInput::LocalImage { path } => format!("[image: {}]", path.display()),
            UserInput::Skill { name, .. } => format!("[skill: {name}]"),
            UserInput::Mention { name, .. } => format!("[mention: {name}]"),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn push_message(
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

#[cfg(test)]
mod tests {
    use codex_app_server_protocol::ThreadItem;
    use codex_app_server_protocol::Turn;
    use codex_app_server_protocol::TurnItemsView;
    use codex_app_server_protocol::TurnStatus;
    use codex_app_server_protocol::UserInput;
    use codex_protocol::ThreadId;
    use codex_utils_absolute_path::AbsolutePathBuf;
    use pretty_assertions::assert_eq;

    use super::message_from_thread_item;
    use crate::remote_control::RemoteControlTranscriptRole;

    #[test]
    fn maps_app_server_user_and_agent_items_to_phone_transcript_roles() {
        let turn = Turn {
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
        };

        let mapped = turn
            .items
            .iter()
            .filter_map(message_from_thread_item)
            .collect::<Vec<_>>();

        assert_eq!(
            mapped,
            vec![
                (
                    RemoteControlTranscriptRole::User,
                    "hello from history".to_string()
                ),
                (
                    RemoteControlTranscriptRole::Assistant,
                    "hello from codex".to_string()
                ),
            ]
        );
    }

    #[test]
    fn maps_app_server_tool_items_to_collapsible_tool_role() {
        let item = ThreadItem::CommandExecution {
            id: "cmd-1".to_string(),
            command: "git status --short".to_string(),
            cwd: absolute_cwd(),
            process_id: None,
            source: Default::default(),
            status: codex_app_server_protocol::CommandExecutionStatus::Completed,
            command_actions: Vec::new(),
            aggregated_output: Some(" M codex-rs/tui/src/remote_control.rs".to_string()),
            exit_code: Some(0),
            duration_ms: None,
        };

        assert_eq!(
            message_from_thread_item(&item),
            Some((
                RemoteControlTranscriptRole::Tool,
                "$ git status --short\n M codex-rs/tui/src/remote_control.rs".to_string()
            ))
        );
    }

    #[test]
    fn snapshot_uses_app_server_thread_cwd_and_turn_history() {
        let thread = codex_app_server_protocol::Thread {
            id: ThreadId::new().to_string(),
            session_id: ThreadId::new().to_string(),
            forked_from_id: None,
            preview: String::new(),
            ephemeral: false,
            model_provider: "openai".to_string(),
            created_at: 0,
            updated_at: 0,
            status: codex_app_server_protocol::ThreadStatus::Idle,
            path: None,
            cwd: absolute_cwd(),
            cli_version: "test".to_string(),
            source: codex_protocol::protocol::SessionSource::Cli.into(),
            thread_source: None,
            agent_nickname: None,
            agent_role: None,
            git_info: None,
            name: None,
            turns: vec![Turn {
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
            }],
        };

        let snapshot =
            crate::remote_control::RemoteControlSnapshot::from_app_server_thread(&thread, None);

        assert_eq!(snapshot.cwd, thread.cwd.display().to_string());
        assert_eq!(snapshot.messages.len(), 1);
        assert_eq!(snapshot.messages[0].text, "existing prompt");
    }

    fn absolute_cwd() -> AbsolutePathBuf {
        AbsolutePathBuf::try_from(std::env::current_dir().expect("cwd"))
            .expect("cwd should be absolute")
    }
}
