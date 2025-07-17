CREATE TABLE dimensions.course_dim (
    coursedimsk integer NOT NULL identity(-1, 1) ENCODE raw
    distkey
,
        courseid character varying(500) NOT NULL ENCODE raw,
        domaindimsk integer NOT NULL ENCODE raw,
        coursetitle character varying(500) NOT NULL ENCODE raw,
        courseavailablefromdate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        coursediscontinuedfromdate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        coursecreatedondate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        coursecreatedbyusername character varying(500) ENCODE raw,
        courseupdatedondate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        courseupdatedbydimsk integer NOT NULL ENCODE raw,
        courseislockout integer NOT NULL ENCODE raw,
        iscourseactive boolean NOT NULL DEFAULT false ENCODE raw,
        displayforlearners boolean NOT NULL DEFAULT false ENCODE raw,
        coursevendordimsk integer NOT NULL ENCODE raw,
        coursecompliance integer NOT NULL ENCODE raw,
        isadhoccourse boolean NOT NULL DEFAULT false ENCODE raw,
        coursedei integer NOT NULL ENCODE raw,
        courseversion character varying(500) NOT NULL ENCODE raw,
        courseconsumableonlywithincurriculum boolean NOT NULL DEFAULT false ENCODE raw,
        courseskillname character varying(500) ENCODE raw,
        coursekeywords character varying(1000) ENCODE raw,
        coursecategoryname character varying(1000) ENCODE raw,
        createddate timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        etlinsertbatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        recordinsertdatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordinsertusername character varying(500) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        etlupdatebatchid bigint NOT NULL DEFAULT -1 ENCODE az64,
        recordupdatedatetimeoffset timestamp with time zone NOT NULL DEFAULT convert_timezone(
            ('UTC':: character varying):: text,
            ('America/New_York':: character varying):: text,
            ('now':: character varying):: timestamp without time zone
        ) ENCODE az64,
        recordupdateusername character varying(500) NOT NULL DEFAULT "current_user"() ENCODE lzo,
        PRIMARY KEY (coursedimsk)
) DISTSTYLE KEY
SORTKEY
    (coursedimsk);