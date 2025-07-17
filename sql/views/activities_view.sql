CREATE
OR REPLACE VIEW "reporting"."vwActivities" AS
SELECT
   r.username,
   t.email,
   tl.email AS leaderemail,
   tll.email AS leadersleaderemail,
   t.preferredfirstname:: text + ' ':: character varying:: text + t.preferredlastname:: text AS fullname,
   tl.preferredfirstname:: text + ' ':: character varying:: text + tl.preferredlastname:: text AS leaderfullname,
   tll.preferredfirstname:: text + ' ':: character varying:: text + tll.preferredlastname:: text AS leadersleaderfullname,
   t.companycode,
   cd.companydesc AS companyname,
   t.companyid,
   t.businessarea,
   t.businessareaorgcode,
   b.classid,
   b.classdeliveryname,
   c.courseid,
   c.courseversion,
   c.coursetitle,
   c.domainname,
   r.registrationdate,
   r.registrationstatusname,
   b.scheduledclassstartdate,
   b.scheduledclassenddate,
   r.completionstatusname,
   r.completiondate AS transscriptcompletiondate,
   r.registrationcancelleddate AS transcriptcancelleddate,
   r.transcriptscore,
   b.classduration,
   r.transcriptid,
   r.transcriptcreatedon AS transcriptcreatedondate,
   r.transcriptupdatedon AS transcriptupdatedondate,
   r.courseassignedon AS courseassignedondate,
   r.courseassignedby AS courseassignedbyname
FROM
   facts.activitiesfact r
   LEFT JOIN dimensions.vwclassdim b ON r.classdimsk = b.classdimsk
   LEFT JOIN dimensions.vwcoursedim c ON r.coursedimsk = c.coursedimsk
   LEFT JOIN dimensions.teammembercoredim t ON t.commonid:: character varying:: text = r.commonid:: text
   LEFT JOIN dimensions.vwteammembercoredim tl ON tl.commonid = t.teamleadercommonid
   LEFT JOIN dimensions.vwteammembercoredim tll ON tll.commonid = tl.teamleadercommonid
   LEFT JOIN dimensions.vwcompanydim cd ON cd.companycode:: text = t.companycode:: text;