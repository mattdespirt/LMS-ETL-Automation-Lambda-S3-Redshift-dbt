CREATE OR REPLACE PROCEDURE upserts.sp_assessmentfact_upsert()
 LANGUAGE plpgsql
AS $_$ 

DECLARE
  etlrunmanager_rec RECORD;

BEGIN

SELECT INTO etlrunmanager_rec * from "dw"."biutilities"."etlrunmanager" where etlprocessname = 'Learning';


--------------assessmentresultdim----------------------------------
drop table if exists stagesrc_assessmentresultdim;

create temp table stagesrc_assessmentresultdim as
select a.*
from 
    (
        select assessment_attempt_summary_status as assessmentresultname,
            row_number() over (partition by assessment_attempt_summary_status order by assessment_attempt_summary_status desc) "rownumber"
        from "dw"."src"."assessments"
        where assessment_attempt_summary_status is not null
    ) a	       
where 
    a.rownumber = 1 ;
 


insert into "dw"."dimensions"."assessmentresultdim" (
    assessmentresultname,
    etlinsertbatchid,
    etlupdatebatchid
)
select 
a.assessmentresultname,
etlrunmanager_rec.etlbatchid,
etlrunmanager_rec.etlbatchid
from stagesrc_assessmentresultdim a
left outer join "dw"."dimensions"."assessmentresultdim" b on b.assessmentresultname = a.assessmentresultname
where b.assessmentresultname is null;
 

drop table if exists stagesrc_assessmentresultdim;

--------------assessmentquestionresultdim----------------------------------
drop table if exists stagesrc_assessmentquestionresultdim;

create temp table stagesrc_assessmentquestionresultdim as
select a.*
from 
    (
        select assessment_question_result as assessmentquestionresultname,
            row_number() over (partition by assessment_question_result order by assessment_question_result desc) "rownumber"
        from "dw"."src"."assessments"
        where assessment_question_result is not null
    ) a	       
where 
    a.rownumber = 1 ;
 


insert into "dw"."dimensions"."assessmentquestionresultdim" (
    assessmentquestionresultname,
    etlinsertbatchid,
    etlupdatebatchid
)
select 
a.assessmentquestionresultname,
etlrunmanager_rec.etlbatchid,
etlrunmanager_rec.etlbatchid
from stagesrc_assessmentquestionresultdim a
left outer join "dw"."dimensions"."assessmentquestionresultdim" b on b.assessmentquestionresultname = a.assessmentquestionresultname
where b.assessmentquestionresultname is null;
 

drop table if exists stagesrc_assessmentquestionresultdim;

--------------assessmenttopicdim----------------------------------
drop table if exists stagesrc_assessmenttopicdim;

create temp table stagesrc_assessmenttopicdim as
select a.*
from 
    (
        select assessment_topic_name as assessmenttopicname,
            row_number() over (partition by assessment_topic_name order by assessment_topic_name desc) "rownumber"
        from "dw"."src"."assessments"
        where assessment_topic_name is not null
    ) a	       
where 
    a.rownumber = 1 ;
 


insert into "dw"."dimensions"."assessmenttopicdim" (
    assessmenttopicname,
    etlinsertbatchid,
    etlupdatebatchid
)
select 
a.assessmenttopicname,
etlrunmanager_rec.etlbatchid,
etlrunmanager_rec.etlbatchid
from stagesrc_assessmenttopicdim a
left outer join "dw"."dimensions"."assessmenttopicdim" b on b.assessmenttopicname = a.assessmenttopicname
where b.assessmenttopicname is null;
 

drop table if exists stagesrc_assessmenttopicdim;

-------------------- upsert to assessmentfact ----------------------
drop table if exists stagesrc_assessmentfact;

create temp table stagesrc_assessmentfact as
select a.*
from 
    (
         select
        a.person_username as commonid,
        COALESCE(b.assessmentdimsk, -1) as assessmentdimsk,
        CASE
            WHEN a.assessment_attempt_number IS NULL OR a.assessment_attempt_number = 'nan' THEN 1
            ELSE CAST(ROUND(a.assessment_attempt_number, 0) AS INTEGER) 
            END as assessmentattemptnumber,
        COALESCE(d.assessmentresultdimsk, -1) as assessmentresultdimsk,
        CASE
            WHEN a.assessment_attempt_summary_score IS NULL OR a.assessment_attempt_summary_score = 'nan' THEN 0
            ELSE CAST(a.assessment_attempt_summary_score AS NUMERIC(10,2)) 
            END as assessmentscore,
        COALESCE(e.assessmenttopicdimsk, -1) as assessmenttopicdimsk,
        a.transcript_internal_id as transcriptid,
        COALESCE(c.assessmentquestiondimsk, -1) as assessmentquestiondimsk,
        a.assessment_question_response as assessmentquestionresponse,
        COALESCE(f.assessmentquestionresultdimsk, -1) as assessmentquestionresultdimsk,
        a.assessment_question_correct_answer as assessmentquestioncorrectanswer,
        CASE 
            WHEN a.assessment_completion_date IS NULL OR a.assessment_completion_date = 'nan' THEN '1753-01-01'
            ELSE COALESCE(CAST(a.assessment_completion_date AS timestamp), '1753-01-01')
                END AS assessmentcompletiondate,      
            row_number() over(partition by a.person_username, b.assessmentdimsk, a.assessment_attempt_number, a.assessment_attempt_summary_score, c.assessmentquestiondimsk, a.transcript_internal_id order by biutilities.fn_getfileprocessedtimestamp("$path") desc) RN
        from src_"assessments" a
        left outer join dimensions.assessmentdim b on b.assessmentid = a.assessment_exam_id and b.assessmenttitle = a.assessment_title
        left outer join dimensions.assessmentquestiondim c on c.assessmentquestionid = a.question_id and c.assessmentquestiontext = a.assessment_question_text
        left outer join dimensions.assessmentresultdim d on d.assessmentresultname = a.assessment_attempt_summary_status
        left outer join dimensions.assessmenttopicdim e on e.assessmenttopicname = a.assessment_topic_name
        left outer join dimensions.assessmentquestionresultdim f on f.assessmentquestionresultname = a.assessment_question_result
        where a.assessment_content_format LIKE '%Test%'
    ) a	       
where 
    a.RN = 1 ;


-- Update existing records
UPDATE rockhuman.assessmentfact
SET
    assessmentresultdimsk = staging.assessmentresultdimsk,
    assessmenttopicdimsk = staging.assessmenttopicdimsk,
    assessmentquestionresponse = staging.assessmentquestionresponse,
    assessmentquestionresultdimsk = staging.assessmentquestionresultdimsk,
    assessmentquestioncorrectanswer = staging.assessmentquestioncorrectanswer,
    assessmentcompletiondate = staging.assessmentcompletiondate,
    etlupdatebatchid = etlrunmanager_rec.etlbatchid,
    recordupdatedatetimeoffset = CONVERT_TIMEZONE('UTC', 'America/New_York', sysdate),
    recordupdateusername = current_user
FROM stagesrc_assessmentfact staging
INNER JOIN rockhuman.assessmentfact f ON staging.transcriptid = f.transcriptid 
AND staging.commonid = f.commonid
AND staging.assessmentdimsk = f.assessmentdimsk
AND staging.assessmentattemptnumber = f.assessmentattemptnumber
AND staging.assessmentscore = f.assessmentscore
AND staging.assessmentquestiondimsk = f.assessmentquestiondimsk;

-- Insert new records
INSERT INTO rockhuman.assessmentfact (
    commonid,
    assessmentdimsk,
    assessmentattemptnumber,
    assessmentresultdimsk,
    assessmentscore,
    assessmenttopicdimsk,
    transcriptid,
    assessmentquestiondimsk,
    assessmentquestionresponse,
    assessmentquestionresultdimsk,
    assessmentquestioncorrectanswer,
    assessmentcompletiondate,
    etlinsertbatchid,
    etlupdatebatchid
)
SELECT
    staging.commonid,
    staging.assessmentdimsk,
    staging.assessmentattemptnumber,
    staging.assessmentresultdimsk,
    staging.assessmentscore,
    staging.assessmenttopicdimsk,
    staging.transcriptid,
    staging.assessmentquestiondimsk,
    staging.assessmentquestionresponse,
    staging.assessmentquestionresultdimsk,
    staging.assessmentquestioncorrectanswer,
    staging.assessmentcompletiondate,
    etlrunmanager_rec.etlbatchid,
    etlrunmanager_rec.etlbatchid
FROM stagesrc_assessmentfact staging
LEFT JOIN rockhuman.assessmentfact main ON staging.commonid = main.commonid 
AND staging.transcriptid = main.transcriptid
AND staging.assessmentdimsk = main.assessmentdimsk
AND staging.assessmentattemptnumber = main.assessmentattemptnumber
AND staging.assessmentscore = main.assessmentscore
AND staging.assessmentquestiondimsk = main.assessmentquestiondimsk
WHERE main.commonid IS NULL;

drop table if exists stagesrc_assessmentfact;

COMMIT;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'dw.upserts.sp_assessmentfact_upsert failed.';
    ROLLBACK;

END
$_$
