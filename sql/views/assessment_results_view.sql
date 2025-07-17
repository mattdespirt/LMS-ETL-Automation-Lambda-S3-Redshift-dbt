CREATE
OR REPLACE VIEW "reporting"."vwAssessmentQuestionResults" AS
SELECT
   a.username,
   rc.classid,
   rc.courseid,
   a.transcriptid,
   b.assessmenttitle,
   b.assessmenttopic,
   a.assessmentattemptnumber,
   c.assessmentresultname,
   a.assessmentscore,
   e.assessmentquestiondimsk,
   e.assessmentquestiontext,
   a.assessmentquestionresponse,
   f.assessmentquestionresultname
FROM
   facts.assessmentfact a
   JOIN reporting."vwActivities" rc ON rc.transcriptid:: text = a.transcriptid:: text
   JOIN dimensions.assessmentdim b ON b.assessmentdimsk = a.assessmentdimsk
   JOIN dimensions.assessmentresultdim c ON c.assessmentresultdimsk = a.assessmentresultdimsk
   JOIN dimensions.assessmentquestiondim e ON e.assessmentquestiondimsk = a.assessmentquestiondimsk
   JOIN dimensions.assessmentquestionresultdim f ON f.assessmentquestionresultdimsk = a.assessmentquestionresultdimsk;