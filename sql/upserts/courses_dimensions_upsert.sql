CREATE OR REPLACE PROCEDURE upserts.sp_coursedim_upsert()
 LANGUAGE plpgsql
AS $_$ 

DECLARE
  etlrunmanager_rec RECORD;

BEGIN

SELECT INTO etlrunmanager_rec * from "dw"."biutilities"."etlrunmanager" where etlprocessname = 'Learning';

--------------------upsert to course dim----------------------
drop table if exists stagesrclearningcoursedim;

create temp table stagesrclearningcoursedim as
select a.*
from 
    (
         select
        course_course_id as courseid,
        course_domain,
        COALESCE(domaindimsk, -1) as domaindimsk,
        course_title as coursetitle,
        CASE 
            WHEN course_available_from IS NULL OR course_available_from = 'nan' THEN '1753-01-01'
            ELSE COALESCE(CAST(course_available_from AS timestamp), '1753-01-01')
                END AS courseavailablefromdate,

        CASE 
            WHEN course_discontinued_from IS NULL OR course_discontinued_from = 'nan' THEN '1753-01-01'
            ELSE COALESCE(CAST(course_discontinued_from AS timestamp), '1753-01-01')
                END AS coursediscontinuedfromdate,

        CASE 
            WHEN course_created_on IS NULL OR course_created_on = 'nan' THEN '1753-01-01'
            ELSE COALESCE(CAST(course_created_on AS timestamp), '1753-01-01')
                END AS coursecreatedondate,
        course_created_by_username as coursecreatedbyusername,
        CASE 
            WHEN course_updated_on IS NULL OR course_updated_on = 'nan' THEN '1753-01-01'
            ELSE COALESCE(CAST(course_updated_on AS timestamp), '1753-01-01')
                END AS courseupdatedondate,
        COALESCE(admindimsk, -1) as courseupdatedbydimsk,
        CASE course_islockout WHEN 'Yes' THEN 1 WHEN 'No' THEN 0 ELSE -1 END as courseislockout,
        CASE is_course_active WHEN 'Active' THEN true ELSE false END as iscourseactive,
        CASE display_for_learners WHEN 'Yes' THEN true ELSE false END as displayforlearners,
        COALESCE(coursevendordimsk, -1) as coursevendordimsk ,
        CASE course_compliance WHEN 'Yes' THEN 1 WHEN 'No' THEN 0 ELSE -1 END as coursecompliance,
        CASE is_adhoc_course WHEN 'Yes' THEN true ELSE false END as isadhoccourse,
        CASE course_dei WHEN 'Yes' THEN 1 WHEN 'No' THEN 0 ELSE -1  END as coursedei,
        CASE
            WHEN course_version IS NULL OR course_version = 'nan' THEN '1'
            ELSE COALESCE(course_version, '1')
                END AS courseversion,
        CASE course_consumable_only_within_curriculum WHEN 'Yes' THEN true ELSE false END as courseconsumableonlywithincurriculum,
        course_skill_name as courseskillname,
        course_keywords as coursekeywords ,
        category_name as coursecategoryname,
            row_number() over(partition by course_course_id order by biutilities.fn_getfileprocessedtimestamp("$path") desc) RN
        from src"courses" c
        left outer join dimensions.learning_domaindim d on c.course_domain = d.domainname
        left outer join dimensions.learning_coursevendordim v on c.course_vendor = v.coursevendorname
        left outer join dimensions.learning_admindim m on c.course_updated_by = m.adminname
        --WHERE course_islockout <> 'No'
    ) a	       
where 
    a.RN = 1 ;


UPDATE dimensions.learning_coursedim
SET
    courseid = staging.courseid,
    domaindimsk = staging.domaindimsk,
    coursetitle = staging.coursetitle,
    courseavailablefromdate = staging.courseavailablefromdate,
    coursediscontinuedfromdate = staging.coursediscontinuedfromdate,
    coursecreatedondate = staging.coursecreatedondate,
    coursecreatedbyusername = staging.coursecreatedbyusername,
    courseupdatedondate = staging.courseupdatedondate,
    courseupdatedbydimsk = staging.courseupdatedbydimsk,
    courseislockout = staging.courseislockout,
    iscourseactive = staging.iscourseactive,
    displayforlearners = staging.displayforlearners,
    coursevendordimsk = staging.coursevendordimsk,
    coursecompliance = staging.coursecompliance,
    isadhoccourse = staging.isadhoccourse,
    coursedei = staging.coursedei,
    courseversion = staging.courseversion,
    courseconsumableonlywithincurriculum = staging.courseconsumableonlywithincurriculum,
    courseskillname = staging.courseskillname,
    coursekeywords = staging.coursekeywords,
    coursecategoryname = staging.coursecategoryname,
    --createddate = staging.createddate,
    --etlinsertbatchid = staging.etlinsertbatchid,
    /*
    recordinsertdatetimeoffset = staging.recordinsertdatetimeoffset,
    recordinsertusername = staging.recordinsertusername,
    */
    etlupdatebatchid = etlrunmanager_rec.etlbatchid,
    recordupdatedatetimeoffset = CONVERT_TIMEZONE ('UTC', 'America/New_York', sysdate),
    recordupdateusername = current_user
    
FROM stagesrclearningcoursedim staging
WHERE dimensions.learning_coursedim.courseid = staging.courseid;


-- Insert new records
INSERT INTO dimensions.learning_coursedim (
    courseid,
    domaindimsk,
    coursetitle,
    courseavailablefromdate,
    coursediscontinuedfromdate,
    coursecreatedondate,
    coursecreatedbyusername,
    courseupdatedondate,
    courseupdatedbydimsk,
    courseislockout,
    iscourseactive,
    displayforlearners,
    coursevendordimsk,
    coursecompliance,
    isadhoccourse,
    coursedei,
    courseversion,
    courseconsumableonlywithincurriculum,
    courseskillname,
    coursekeywords,
    coursecategoryname,
    --createddate,
    etlinsertbatchid,
    --recordinsertdatetimeoffset,
    --recordinsertusername,
    etlupdatebatchid
    --recordupdatedatetimeoffset,
    --recordupdateusername
)
SELECT
    staging.courseid,
    staging.domaindimsk,
    staging.coursetitle,
    staging.courseavailablefromdate,
    staging.coursediscontinuedfromdate,
    staging.coursecreatedondate,
    staging.coursecreatedbyusername,
    staging.courseupdatedondate,
    staging.courseupdatedbydimsk,
    staging.courseislockout,
    staging.iscourseactive,
    staging.displayforlearners,
    staging.coursevendordimsk,
    staging.coursecompliance,
    staging.isadhoccourse,
    staging.coursedei,
    staging.courseversion,
    staging.courseconsumableonlywithincurriculum,
    staging.courseskillname,
    staging.coursekeywords,
    staging.coursecategoryname,
    --staging.createddate,
    --staging.etlinsertbatchid,
    etlrunmanager_rec.etlbatchid,
    --staging.recordinsertdatetimeoffset,
    --staging.recordinsertusername,
    etlrunmanager_rec.etlbatchid
    --staging.etlupdatebatchid,
    --staging.recordupdatedatetimeoffset,
    --staging.recordupdateusername
FROM stagesrclearningcoursedim staging
LEFT JOIN dimensions.learning_coursedim main
ON staging.courseid = main.courseid
WHERE main.courseid IS NULL;



COMMIT;

EXCEPTION WHEN OTHERS THEN
    RAISE INFO 'dw.upserts.sp_coursedim_upsert failed.';
    ROLLBACK;

END
$_$
