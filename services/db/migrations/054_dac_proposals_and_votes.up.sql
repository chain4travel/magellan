CREATE TABLE `dac_proposals` (
  id    VARCHAR(50)      NOT NULL PRIMARY KEY,
  proposer_addr  VARCHAR(50)      NOT NULL,
  start_time     TIMESTAMP        NOT NULL,
  end_time       TIMESTAMP        NOT NULL,
  type           TINYINT          NOT NULL,
  options        VARBINARY(1024)  NOT NULL,
  outcome        VARBINARY(1024),
  status         TINYINT          NOT NULL
);
 
CREATE INDEX dac_proposals_by_id ON dac_proposals (id);


CREATE TABLE `dac_votes` (
  id             VARCHAR(50)      NOT NULL PRIMARY KEY,
  voter_addr     VARCHAR(50)      NOT NULL,
  voted_at       TIMESTAMP        NOT NULL,
  proposal_id    VARCHAR(50)      NOT NULL,
  voted_options  VARBINARY(1024)  NOT NULL
);
 
 
CREATE INDEX dac_votes_by_proposal_id ON dac_votes (proposal_id);