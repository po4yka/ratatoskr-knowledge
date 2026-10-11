## ADDED Requirements

### Requirement: A conversation is indexed as its text

The search document of an AI-archive conversation SHALL have the conversation title as `title`, the plain text of its messages as `body` and the first message's text cut at 280 characters as `lead`, and SHALL contain no identifiers, role labels or prompt scaffold.

#### Scenario: Scaffold tokens are not searchable

- **WHEN** a conversation is analysed and searched for `conversation_id`, `message_id`, `family` and `role`
- **THEN** nothing is returned, while a word from a message returns the conversation with a snippet of message text
