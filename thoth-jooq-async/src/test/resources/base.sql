
CREATE TABLE IF NOT EXISTS vikings_journal (
  id UUID primary key,
  entity_id varchar(100) not null,
  sequence_num bigint not null,
  event_type varchar(100) not null,
  version int not null,
  transaction_id varchar(100) not null,
  event jsonb not null,
  metadata jsonb,
  context jsonb,
  total_message_in_transaction int default 1,
  num_message_in_transaction int default 1,
  emission_date timestamp not null default now(),
  user_id varchar(100),
  system_id varchar(100),
  published boolean default false,
  UNIQUE (entity_id, sequence_num)
);
CREATE INDEX IF NOT EXISTS vikings_sequence_num_idx ON vikings_journal (sequence_num);
CREATE INDEX IF NOT EXISTS vikings_entity_id_idx ON vikings_journal (entity_id);
CREATE INDEX IF NOT EXISTS vikings_user_id_idx ON vikings_journal (user_id);
CREATE INDEX IF NOT EXISTS vikings_system_id_idx ON vikings_journal (system_id);
CREATE INDEX IF NOT EXISTS vikings_emission_date_idx ON vikings_journal (emission_date);
CREATE SEQUENCE if not exists vikings_sequence_num;
CREATE SEQUENCE if not exists vikings_id;
