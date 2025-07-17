CREATE TABLE facts.activity_fact (
    activity_factid bigint NOT NULL identity(1, 1) ENCODE az64
    distkey
,
        username character varying(500) NOT NULL ENCODE lzo,
        classdimsk integer NOT NULL ENCODE raw,
        coursedimsk integer NOT NULL ENCODE raw,
        regstatusdimsk integer NOT NULL ENCODE raw,
        compstatusdimsk integer NOT NULL ENCODE raw,
        courseassignedon timestamp with time zone DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        registrationdate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        registrationcancelleddate timestamp with time zone DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        completiondate timestamp with time zone DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        transcriptscore character varying(500) ENCODE lzo,
        islearningmandatory boolean NOT NULL DEFAULT false ENCODE raw,
        transcriptid character varying(500) NOT NULL ENCODE lzo,
        transcriptcreatedon timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        transcriptupdatedon timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        courseassignedby character varying(500) ENCODE lzo,
        etlinsertbatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        recordinsertdatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordinsertusername character varying(100) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        etlupdatebatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        recordupdatedatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordupdateusername character varying(100) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        PRIMARY KEY (activity_factid)
) DISTSTYLE KEY
SORTKEY
    (activity_factid);