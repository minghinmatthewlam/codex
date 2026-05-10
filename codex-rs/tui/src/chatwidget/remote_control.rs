use super::ChatWidget;
use super::UserMessage;

impl ChatWidget {
    pub(crate) fn submit_remote_control_user_message(&mut self, text: String) {
        self.submit_user_message(UserMessage::from(text));
    }
}
