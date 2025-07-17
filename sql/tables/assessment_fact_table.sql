CREATE TABLE facts.assessment_fact (
    assessment_factid integer NOT NULL identity(-1, 1) ENCODE raw
    distkey
,
        username character varying(500) NOT NULL ENCODE raw,
        assessmentdimsk integer NOT NULL ENCODE raw,
        assessmentattemptnumber integer ENCODE raw,
        assessmentresultdimsk integer NOT NULL ENCODE raw,
        assessmentscore numeric(18, 2) ENCODE raw,
        assessmenttopicdimsk integer ENCODE raw,
        transcriptid character varying(500) NOT NULL ENCODE raw,
        assessmentquestiondimsk integer NOT NULL ENCODE raw,
        assessmentquestionresponse character varying(10000) ENCODE raw,
        assessmentquestionresultdimsk integer NOT NULL ENCODE raw,
        assessmentquestioncorrectanswer character varying(10000) ENCODE raw,
        assessmentcompletiondate timestamp with time zone DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordinsertdatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordinsertusername character varying(500) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        etlinsertbatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        etlupdatebatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        recordupdatedatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordupdateusername character varying(500) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        PRIMARY KEY (assessment_factid)
) DISTSTYLE KEY
SORTKEY
    (assessment_factid);