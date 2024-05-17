PROCEDURE CR_SC_MTD_AM (
    per in VARCHAR2,vfusionid5 in VARCHAR2, p_result OUT SYS_REFCURSOR) 
    is 
    period date;
  BEGIN
    -- TODO: Implementation required for PROCEDURE CR_SC_MTD_AGNTS.CR_SC_MTD_AGNTS
      period:=to_date(per,'yyyy-MM-dd');
      --vfusionid:='196035';
         dbms_output.put_line('period =' || period);
         
         
     if(vfusionid5 is null and period is null ) then
            OPEN p_result FOR  
  
 select  
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."EMPLOYEE ID",
a."TL NAME",
a."ASM",
a."LOCATION",
a."DEPT",
a."Overall Rank",
a."Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Quality Score",a."Quality_Score",a.QA_TARGET,
a."Out Off",
a."YTD Global Rank",
a."YTD Global Total Points"
 from(
 select 
a."MONTH",
a."UCID",
a."FUSION_ID" "EMPLOYEE ID",
a."ASM_NAME" "TL NAME",
a."MANAGER" "ASM",
a."LOCATION",
a."DEPT",
a."Overall Rank",
a."Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Quality Score",a."Quality_Score",a.QA_TARGET,
a."Out Off",
--'' "YTD Global Rank",
--'' "YTD Global Total Score"
b."YTD Global Rank",
b."YTD Global Total Points"  "YTD Global Total Points",
rank() over ( order by b."YTD Global Total Points" desc ) sn
from(
select 
a."MONTH",
a."UCID",
a."FUSION_ID",
a."ASM_NAME",
a."MANAGER",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target
,e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"


) a 
left join (
select 
a."EMPLOYEE ID",a."Global Rank" "YTD Global Rank",a."Total Points" "YTD Global Total Points"
from (
select 
a."EMPLOYEE ID",
a."ASM_NAME" "TL NAME",
upper(c.team_leader) "ASM",
c.location,
a."DEPT",
a."Global Rank",
a."Total Points",
a."Credit Per Hr",
a."Inbound AHT",
a."Cms Defect %",
a."TL Call Monitoring Defect",
a."Collection call Model Defect",
case when a."Global Rank" <= round(b."Out_of"*0.10,0) then 1 
 when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 2 
  when a."Global Rank" <= (round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 3 
  when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 4 
  else 5
 end TIER,
b."Out_of"
from(
select 
/*a."MONTH",
a."UCID",*/
a."FUSION_ID" "EMPLOYEE ID",
upper(a."ASM_NAME") "ASM_NAME",
/*a."MANAGER",
a."LOCATION",*/
a."DEPT",
rank() OVER (order by sum(a."Overall Score") desc,a."FUSION_ID") "Global Rank",
sum(a."Overall Score") "Total Points",
round(avg(a."Resolution Credits"),3) "Credit Per Hr" ,
--a."Res Credit Rank",
--a."Res Credit Points",
--a."CPH_TARGET",
round(avg(a."Inbound AHT"),3) "Inbound AHT",
--a."AHT Points",
--a."IB_AHT_TARGET",
round(avg(a."Cms Defect %"),3) "Cms Defect %",
--a."CMS Usage Points",
--a."CMS_DEFECT_TARGET",
round(avg(a."TL Call Monitoring Defect"),3) "TL Call Monitoring Defect",
--a."TL Call Monitoring Points",
--a."CALL_MONITORING_DEFECT_TARGET",
round(avg(a."Collection call Model Defect"),3)  "Collection call Model Defect", 
--a."Collection call Model Points",
--a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
round(avg(a."Quality Score"),3) "Quality Score"
--,a."Quality_Score",a.QA_TARGET,
--a."Out Off"*/
/*,case when a."Global Rank" <= round(b."Out_of"*0.10,0) then 1 
 when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 2 
  when a."Global Rank" <= (round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 3 
  when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 4 
  else 5
 end TIER
--,b."Out_of"*/
from(
 select 
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."FUSION_ID",
a."ASM_NAME",
a."MANAGER",
a."LOCATION",
a."DEPT",
a."Overall Rank",
a."Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Quality Score",a."Quality_Score",a.QA_TARGET,
a."Out Off",
'' "YTD Global Rank",
'' "YTD Global Total Score"
--b."YTD Global Rank",
--b."YTD Global Total Score"
from(


select 
a."MONTH",
a."UCID",
a."FUSION_ID",
a."ASM_NAME",
a."MANAGER",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target,
e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"

) a 
--where a."MONTH" >= last_day(period) and 
--a."FUSION_ID" in (vfusionid5)
--and a."FUSION_ID"='100713'
--order by a.month asc;  

 ) a 
 group by a."FUSION_ID",a."ASM_NAME",a."DEPT"
 )a
 
left join (
 select  
a.dept,
count(a."Global Rank") "Out_of"
from( 
select 
/*a."MONTH",
a."UCID",*/
a."FUSION_ID" "EMPLOYEE ID",
upper(a."ASM_NAME") "ASM_NAME",
/*a."MANAGER",
a."LOCATION",*/
a."DEPT",
rank() OVER (order by sum(a."Overall Score") desc,a."FUSION_ID") "Global Rank",
sum(a."Overall Score") "Total Points",
round(avg(a."Resolution Credits"),3) "Credit Per Hr" ,
--a."Res Credit Rank",
--a."Res Credit Points",
--a."CPH_TARGET",
round(avg(a."Inbound AHT"),3) "Inbound AHT",
--a."AHT Points",
--a."IB_AHT_TARGET",
round(avg(a."Cms Defect %"),3) "Cms Defect %",
--a."CMS Usage Points",
--a."CMS_DEFECT_TARGET",
round(avg(a."TL Call Monitoring Defect"),3) "TL Call Monitoring Defect",
--a."TL Call Monitoring Points",
--a."CALL_MONITORING_DEFECT_TARGET",
round(avg(a."Collection call Model Defect"),3)  "Collection call Model Defect", 
--a."Collection call Model Points",
--a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
round(avg(a."Quality Score"),3) "Quality Score"
--,a."Quality_Score",a.QA_TARGET,
--a."Out Off"*/
/*,case when a."Global Rank" <= round(b."Out_of"*0.10,0) then 1 
 when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 2 
  when a."Global Rank" <= (round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 3 
  when a."Global Rank" <= (round(b."Out_of"*0.20,0)+round(b."Out_of"*0.40,0)+round(b."Out_of"*0.20,0)+round(b."Out_of"*0.10,0)) then 4 
  else 5
 end TIER
--,b."Out_of"*/
from(
 select 
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."FUSION_ID",
a."ASM_NAME",
a."MANAGER",
a."LOCATION",
a."DEPT",
a."Overall Rank",
a."Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Quality Score",a."Quality_Score",a.QA_TARGET,
a."Out Off",
'' "YTD Global Rank",
'' "YTD Global Total Score"
--b."YTD Global Rank",
--b."YTD Global Total Score"
from(


select 
a."MONTH",
a."UCID",
a."FUSION_ID",
a."ASM_NAME",
a."MANAGER",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target,
e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"

) a 
--where a."MONTH" >= last_day(period) and 
--a."FUSION_ID" in (vfusionid5)
--and a."FUSION_ID"='100713'
--order by a.month asc;  

 ) a 
 group by a."FUSION_ID",a."ASM_NAME",a."DEPT"
)a 
group by a.dept

)b on  a.dept=b.dept
left join (

select month,fusionid,ucid,name,team_leader,location,dept,designation from (
select month,fusionid,ucid,upper(name) name,upper(team_leader) team_leader,location,dept,designation ,
row_number() over  ( partition by fusionid order by month desc ) sn 
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
) where sn=1

)c on a."EMPLOYEE ID"=c.fusionid
 
 )a
)b on a."FUSION_ID"=b."EMPLOYEE ID"
) a 
where sn=1
--where a."MONTH" >= last_day(period) and 
--a."FUSION_ID" in (vfusionid5)
--and a."FUSION_ID"='100713'
order by a.month asc;  




   elsif (vfusionid5 is null) then
            OPEN p_result FOR
            
 select 
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."FUSION_ID" "EMPLOYEE ID",
a."ASM_NAME" "TL NAME",
a."MANAGER" "ASM",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off",
'' "YTD Global Rank",
'' "YTD Global Total Points"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target
,e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"
 
where a."MONTH" >= last_day(period)
--and a."FUSION_ID"='100713'
order by a.month asc;  


elsif (period is null) then
            OPEN p_result FOR
            
 select 
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."FUSION_ID" "EMPLOYEE ID",
a."ASM_NAME" "TL NAME",
a."MANAGER" "ASM",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off",
'' "YTD Global Rank",
'' "YTD Global Total Points"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target
,e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"
where --a."MONTH" >= last_day(period) and 
a."FUSION_ID" in (vfusionid5)
--and a."FUSION_ID"='100713'
order by a.month asc;    


            else
            OPEN p_result FOR 
            
select 
to_char(a."MONTH",'MON-YY') MONTH,
a."UCID",
a."FUSION_ID" "EMPLOYEE ID",
a."ASM_NAME" "TL NAME",
a."MANAGER" "ASM",
a."LOCATION",
a."DEPT",
rank() OVER (PARTITION by a."MONTH",a."DEPT" ORDER BY (round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2)) desc,a."MONTH",a."DEPT" ) "Overall Rank",
(round( coalesce(a."Res Credit Points",0) + coalesce(a."AHT Points",0) + coalesce(a."CMS Usage Points",0)+ coalesce(a."TL Call Monitoring Points",0) +
coalesce(a."Collection call Model Points",0)+coalesce(b."Quality_Score",0)
,2))"Overall Score",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Inbound AHT",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
b."Quality Score",b."Quality_Score",a.QA_TARGET,
a."Out Off",
'' "YTD Global Rank",
'' "YTD Global Total Points"
from (
select 
a.month,c.ucid,a."TL FUSION_ID" "FUSION_ID",c.name "ASM_NAME",c.team_leader "MANAGER",c.location,c.dept,
round(avg(a."Resolution Credits"),2) "Resolution Credits",
rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) "Res Credit Rank",
case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept ) =1 
then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )=2
then 5
else 2.5 end as "Res Credit Points",
/*case when round(avg(a."Resolution Credits"),2) < 2 then 0 else
(case when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.15,0) then 7.5
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.6,0) then 5 
when rank() OVER (PARTITION by a.month,c.dept ORDER BY round(avg(a."Resolution Credits"),3) desc,a.month,c.dept )
<=round(d."Out Off"*0.85,0) then 2.5 
else 0 end )
end as  "Res Credit Points",*/
e.cph_target,
round(avg(a."Inbound AHT"),2) "Inbound AHT",

case when round(avg(a."Inbound AHT"),2) > 660 then 0
 when round(avg(a."Inbound AHT"),2) >=660 then 5-(5*0.80)
when round(avg(a."Inbound AHT"),2) >=630 then 5-(5*0.60)
when round(avg(a."Inbound AHT"),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points",
e.ib_aht_target,
round(avg(a."Cms Defect %"),2) "Cms Defect %",
case when round(avg(a."Cms Defect %"),2) <0.5 then 18.75
when round(avg(a."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(a."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points",

e.cms_defect_target,
round(avg(a."TL Call Monitoring Defect"),2) "TL Call Monitoring Defect",
case when round(avg(a."TL Call Monitoring Defect"),2) =0 then 3.75
 when round(avg(a."TL Call Monitoring Defect"),2) <= 25 then 2.5 
  when round(avg(a."TL Call Monitoring Defect"),2) <=75  then 1.25
else 0
end as "TL Call Monitoring Points",
e.call_monitoring_defect_target,
round(avg(a."Collection call Model Defect"),2) "Collection call Model Defect",
case when round(avg(a."Collection call Model Defect"),2) < 1.5 then 7.5
when round(avg(a."Collection call Model Defect"),2) < 2.5 then 5
when round(avg(a."Collection call Model Defect"),2) < 3.5 then 2.5
else 0
end "Collection call Model Points",
e.collection_call_model_defect_target
,e.QA_TARGET,
d."Out Off"
from (
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."SUPV_NAME",a."TL FUSION_ID",
a."MANAGER_NAME",
a."LOCATION",
a."DEPT",
a."Overall Score",
a."Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from(
select 
a."MONTH",
a."FUSION_ID",
a."UCID",
a."NAME" "SUPV_NAME",a."TL FUSION_ID",
a."TEAM_LEADER" "MANAGER_NAME",
a."LOCATION",
a."DEPT",
round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2)  "Overall Score",
rank() OVER (PARTITION by month ORDER BY round((a."Res Credit Points"+a."Stella Star Points"+a."Adherence Points"+a."AHT Points"+a."CMS Usage Points"
+a."TL Call Monitoring Points"+a."Collection call Model Points"),2) desc,month ) "Overall Rank",
a."Resolution Credits",
a."Res Credit Rank",
a."Res Credit Points",
a."CPH_TARGET",
a."Stella Star Rating",
a."Stella Star Rank",
a."Stella Star Points",
a."STAR_TARGET",
a."Schedule Adherence",
a."Adherence Rank",
a."Adherence Points",
a."ADHERENCE_TARGET",
a."Inbound AHT",
a."AHT Rank",
a."AHT Points",
a."IB_AHT_TARGET",
a."Cms Defect %",
a."Cms Defect Rank",
a."CMS Usage Points",
a."CMS_DEFECT_TARGET",
a."TL Call Monitoring Defect",
a."TL Call Monitoring Points",
a."CALL_MONITORING_DEFECT_TARGET",
a."Collection call Model Defect",
a."Collection call Model Points",
a."COLLECTION_CALL_MODEL_DEFECT_TARGET",
a."Out Of"
from (
select 
--"EMPLOYEE ID"--,name,"Team Lead",location,dept
b.month,b.TL_FUSIONID "FUSION_ID",
c.ucid
,b."Team Lead" NAME,c.TL_FUSIONID "TL FUSION_ID", c.team_leader,c.location,c.dept
,round(avg(b."Credit Per Hr"),3) "Resolution Credits"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept ) "Res Credit Rank"
,case when round(avg(b."Credit Per Hr"),3) >2.000 then 
(case when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.15,0) then 11.25 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.6,0) then 7.5 
when rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Credit Per Hr"),3) desc,b.month,c.dept )
<=round(k."Out Of"*0.85,0) then 3.75 
else 0 end )
else 0
end as "Res Credit Points"
,i.cph_target
--,round(avg(b."Quality Score"),2) "Quality Score"
--,i.QUALITY_TARGET
,round(avg(b."Stella Star Rating"),2) "Stella Star Rating"
,rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Stella Star Rating"),2) desc,b.month,c.dept ) "Stella Star Rank"
, case when round(avg(b."Stella Star Rating"),2) >4.6 then  5-(5*0.20)
when round(avg(b."Stella Star Rating"),2) >=4.5 then  5-(5*0.40)
when round(avg(b."Stella Star Rating"),2) >=4.4 then  5-(5*0.60)
when round(avg(b."Stella Star Rating"),2) >=4.3 then  5-(5*0.80)
else 0 
end as "Stella Star Points"
,i.STAR_TARGET
,round(avg(b."Schedule Adherence"),2) "Schedule Adherence"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Schedule Adherence"),2) desc,b.month,c.dept ) "Adherence Rank"
, case when round(avg(b."Schedule Adherence"),3)>92.50 then 5-(5*0.20)
when round(avg(b."Schedule Adherence"),3)>=90.00 then 5-(5*0.40)
when round(avg(b."Schedule Adherence"),3)>=87.50 then 5-(5*0.60)
when round(avg(b."Schedule Adherence"),3)>=85.00 then 5-(5*0.80)
else 0 end as "Adherence Points"
,i.ADHERENCE_TARGET
--,round(avg(b."Inbound AHT"),2) "Inbound AHT"
,coalesce (j."Inbound AHT",0) "Inbound AHT"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(coalesce (j."Inbound AHT",0),2) desc,b.month,c.dept ) "AHT Rank"
,case when round(coalesce (j."Inbound AHT",0),2)>660 then 0
 when round(coalesce (j."Inbound AHT",0),2) >=660 then 5-(5*0.80)
when round(coalesce (j."Inbound AHT",0),2) >=630 then 5-(5*0.60)
when round(coalesce (j."Inbound AHT",0),2) >=600 then 5-(5*0.40)
else 5-(5*0.20) end as "AHT Points"
,i.ib_aht_target
,round(avg(b."Cms Defect %"),2) "Cms Defect %"
, rank() OVER (PARTITION by b.month,c.dept ORDER BY round(avg(b."Cms Defect %"),2) asc,b.month,c.dept ) "Cms Defect Rank"
,case when round(avg(b."Cms Defect %"),2) <0.5 then 18.75
when round(avg(b."Cms Defect %"),2) < 1.0 then 12.5
when round(avg(b."Cms Defect %"),2) <1.5 then 6.25
else 0
end as "CMS Usage Points"
,i.CMS_DEFECT_TARGET
,f.qc_per "TL Call Monitoring Defect"
,case when f.qc_per = 0 then  7.5
when f.qc_per <= 25 then  5
when f.qc_per <= 75 then  2.5
else 0
end as  "TL Call Monitoring Points"
,i.call_monitoring_defect_target
,e.defect_per "Collection call Model Defect"
,case when e.defect_per < 1.5 then 7.5
when e.defect_per < 2.5 then 5
when e.defect_per < 3.5 then 2.5
else 0
end as "Collection call Model Points"
,i.collection_call_model_defect_target
,k."Out Of"
from
(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a.TL_FUSIONID,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") desc,a.dept,a.month ) "Global Rank"
,(a."Credits Score"+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+a."Adherence Score"+a."AHT Score"
+a."Cms Defect Score") "Total Points"
,coalesce(a."Credit Per Hr",0) "Credit Per Hr" ,a."Credit Rank",a."Credits Score",a.CPH_TARGET
, coalesce (a."Quality Score",0)  "Quality Score"
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,coalesce(a."Stella Star Rating",0) "Stella Star Rating"
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,coalesce(a."Schedule Adherence",0) "Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,coalesce(a."Inbound AHT",0) "Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,coalesce(a."Cms Defect %",0) "Cms Defect %" ,a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
a.TL_FUSIONID,
a.team_leader "Team Lead",
a.location,
a.dept
--,rank() OVER (PARTITION by last_day(a.month),a.dept ORDER BY (coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0)) desc,a.dept,last_day(a.month) ) "Global Rank"
--,(coalesce(b."Credits Score",0)+coalesce(c."Quality_Score",0)+coalesce(d."Stella Star Score",0)+coalesce(e."Adherence Score",0)+coalesce(f."AHT Score",0)+coalesce(g."Cms Defect Score",0))  "Total Points"
,b."Credit Per Hr",b."Credit Rank",b."Credits Score",
--b.CPH_TARGET
i.CPH_TARGET
,c."Quality Score",c."Quality Rank",c."Quality_Score"
,i.QUALITY_TARGET
,d."Stella Star Rating",d."Stella Star Rank",d."Stella Star Score"
,i.STAR_TARGET
,e."Schedule Adherence",e."Adherence Rank",e."Adherence Score"
,i.ADHERENCE_TARGET
,f."Inbound AHT",f."AHT Rank",f."AHT Score"
,i.IB_AHT_TARGET
,g."Cms Defect %",g."Cms Defect Rank",g."Cms Defect Score"
,i.CMS_DEFECT_TARGET
,h."Out Of"
from asit_roster_table a
left join(
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
--,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
,"CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
,e."CPH_TARGET"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
left join(
select month,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95","CPH_TARGET"
from (
select month,fusionid,ucid,emp_name,dept,"Credit Per Hr","Credit Rank","Credits Score","Out Of","Out of 95"
,case when cast("Out of 95" as number)=cast("Credit Rank" as number) then "Credit Per Hr" end as "CPH_TARGET"
from (
select last_day(a.rpt_month) month,c.fusionid,a.ucid,a.emp_name,c.dept,cast(coalesce(max(a.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) ) "Credit Rank"
,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.15) then 18.75 
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.6) then 12.50
when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(max(a.credits_per_hour),0) desc,c.dept,last_day(a.rpt_month) )
<=round(b."Out Of"*0.95) then 6.25 else 0
end as "Credits Score"
--,d.cph_target
--,case when rank() OVER (PARTITION by last_day(a.rpt_month),c.dept ORDER BY coalesce(round(max(a.credits_per_hour),1),0) desc,c.dept,last_day(a.rpt_month) )=round(b."Out Of"*0.95) then coalesce(round(max(a.credits_per_hour),1),0) else '' end as   "Target"
,b."Out Of", round(b."Out Of"*0.95) "Out of 95"
from asit_res_credits a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.rpt_month)=last_day(c.month) and a.ucid=c.ucid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.rpt_month)=b.monthn and c.dept=b.dept
left join cr_sc_agnts_target d on last_day(a.rpt_month)=last_day(d.month) and c.dept=d.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of"
)a )b
where "CPH_TARGET" is not null)e on last_day(a.rpt_month)=last_day(e.month) and c.dept=e.dept
group by last_day(a.rpt_month),c.fusionid,a.ucid,a.emp_name,c.dept,d.cph_target,b."Out Of",e."CPH_TARGET"
)a
) b on last_day(a.month)=last_day(b.month) and a.fusionid=b.fusionid
left join(
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,c.dept,b."Out Of"

)c on last_day(a.month)=last_day(c.month) and a.fusionid=c.fusion_id
left join(
select 
last_day(a.month) month,a.employee_custom_id,c.dept
--,a.team_leader
,coalesce(round(avg(star_rating),2),0) "Stella Star Rating"
,rank() OVER (PARTITION by last_day(a.month),c.dept  ORDER BY coalesce(round(avg(star_rating),2),0) desc,c.dept ,last_day(a.month) ) "Stella Star Rank"
,b."Out Of"
,case when coalesce(round(avg(star_rating),2),0) >= 4.6 then 4.00
when coalesce(round(avg(star_rating),2),0) >= 4.5 then 3.00 
when coalesce(round(avg(star_rating),2),0) >= 4.4 then 2.00
when coalesce(round(avg(star_rating),2),0) >= 4.3 then 1.00
else 0
end as "Stella Star Score"
from asit_star_rating a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.employee_custom_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
--where  a.month=last_day(a.month)
--and employee_custom_id in ('196400') 
group by last_day(a.month),c.dept,a.employee_custom_id,b."Out Of"
)d on last_day(a.month)=last_day(d.month) and a.fusionid=d.employee_custom_id
left join (
select 
last_day(a.month) month,a.fusion_id,c.dept 
,coalesce(round(adherence_percentage*100,2),0) "Schedule Adherence"
,rank() OVER (PARTITION by last_day(a.month),c.dept ORDER BY coalesce(round(adherence_percentage*100,2),0) desc,c.dept ,last_day(a.month) ) "Adherence Rank"
,b."Out Of"
,case when coalesce(round(adherence_percentage*100,2),0) >=95 then 4.00 
when coalesce(round(adherence_percentage*100,2),0) >=92.5 then 3.00
when coalesce(round(adherence_percentage*100,2),0) >=90 then 2.00 
when coalesce(round(adherence_percentage*100,2),0) >=87.5 then 1.00
else 0
end as
"Adherence Score"
from asit_telephony a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.fusion_id=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)e on last_day(a.month)=last_day(e.month) and a.fusionid=e.fusion_id
left join (
select
a.month,c.fusionid,a.logon_id,a.emp_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.logon_id,a.emp_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.logon_id,a.emp_name,a.direction 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and upper(a.logon_id)=upper(c.livevox_id)  --and a.emp_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept
)f on to_char(last_day(a.month),'MON-YY')=f.month and a.fusionid=f.fusionid
left join(
select 
last_day(a.month) month,c.fusionid,a.ucid,c.dept,a.cms_defect_percentage,coalesce(round(a.cms_defect_percentage*100,2),0) "Cms Defect %"
,rank() OVER (PARTITION by a.month ORDER BY coalesce(round(a.cms_defect_percentage*100,2),0) asc,a.month ) "Cms Defect Rank"
,b."Out Of"
,case when coalesce(round(a.cms_defect_percentage*100,2),0) <0.5 then 18.5 
when coalesce(round(a.cms_defect_percentage*100,2),0) <1 then 12.5
when coalesce(round(a.cms_defect_percentage*100,2),0) <=1.5 then 6.25
else 0
end as
"Cms Defect Score"
from asit_cms_defect a
join(
select month,fusionid,ucid,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a.ucid=c.ucid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on a.month=b.monthn and c.dept=b.dept
)g on last_day(a.month)=g.month and a.fusionid=g.fusionid
left join(
select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)h on last_day(a.month)=h.monthn and a.dept=h.dept
left join CR_SC_AGNTS_TARGET i on last_day(a.month)=i.month and a.dept=i.dept
where --a.month>=last_day(sysdate)and
 a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
)a)b
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on b.month=c.month and b.TL_FUSIONID=c.fusionid
join (select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)d on b.month=d.monthn and c.dept=d.dept
left join (
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end defect_per 
 from asit_tl_impact
)e on b.month=e.month and b.TL_FUSIONID=e.fusionid
left join(
select MONTH,FUSIONID,UCID,NAME,ASST_MNGR_NAME,MNGR_NAME,LOCATION,DEPT,OPPORTUNITIES,DEFECT
,case when defect=0 then 0 else round((DEFECT/OPPORTUNITIES)*100,3) end QC_per 
 from asit_tl_qc
)f on b.month=f.month and b.TL_FUSIONID=f.fusionid
left join CR_SC_TL_TARGET i on b.month=i.month and c.dept=i.dept
left join(
select
a.month,c.fusionid,a.spvr_name,c.dept,coalesce(a.AHT,0) "Inbound AHT",
rank() OVER (PARTITION by a.month,c.dept ORDER BY coalesce(a.AHT,0) asc,c.dept,a.month ) "AHT Rank"
,b."Out Of"
,case when coalesce(a.AHT,0)<=600 then 7.5
when coalesce(a.AHT,0)<=630 then 5
when coalesce(a.AHT,0)<=660 then 2.5
else 0
end as
"AHT Score"
from (
select to_char(a.month,'MON-yy') month
--,c.fusionid
,a.spvr_name,sum(a.calls_handled),sum(a.handle_time),
round(sum(a.handle_time)/sum(a.calls_handled),2) AHT
,a.direction 
from asit_aht a
where
a.direction='Inbound'
--and a.emp_name in(select name from asit_roster_table where fusionid='193222')
group by to_char(a.month,'MON-yy'),a.direction,a.spvr_name 
) a 
join(
select to_char(month,'MON-yy') month,fusionid,ucid,ntid,livevox_id,name,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on a.month=c.month --and upper(a.logon_id)=upper(c.livevox_id) 
and a.spvr_name =c.name 
left join (select 
to_char(last_day(month),'MON-yy') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-yy'),dept
)b on a.month=b.monthn and c.dept=b.dept

) j on to_char(b.month,'MON-YY')=j.month and b.TL_FUSIONID=j.fusionid
left join (select 
to_char(last_day(month),'MON-YY') monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by to_char(last_day(month),'MON-YY'),dept
)k on to_char(b.month,'MON-YY')=k.monthn and c.dept=k.dept
--where  "EMPLOYEE ID" in('197233')
group by b.month,b.TL_FUSIONID,b."Team Lead"
,c.fusionid,c.ucid,c.TL_FUSIONID ,c.team_leader,c.location,c.dept,
i.cph_target,i.STAR_TARGET,i.ADHERENCE_TARGET,i.CMS_DEFECT_TARGET,i.collection_call_model_defect_target,i.call_monitoring_defect_target,i.ib_aht_target
,e.defect_per,f.QC_per,j."Inbound AHT",k."Out Of"
)a )a ) a 
join(
select month,fusionid,ucid,name,TL_FUSIONID,team_leader,location,dept,designation from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on a.month=c.month and a."TL FUSION_ID"=c.fusionid
join(
select month,dept,count(fusionid) "Out Off" from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
group by  month,dept
)d on a.month=d.month
left join CR_SC_AM_TARGET e on a.month=e.month and c.dept=e.dept
--where a.month>=last_day('01-JAN_24')
--where a."FUSION_ID" in ('195869')
group by a.month,a."TL FUSION_ID",a."MANAGER_NAME",c.ucid,c.name,c.team_leader,c.location,c.dept,d."Out Off"
,e.cph_target,e.ib_aht_target,e.cms_defect_target,e.call_monitoring_defect_target,e.collection_call_model_defect_target
,e.QA_TARGET
)a 
left join (
select 
a.month,
a.tl_fusionid "fusion_id",
a.team_leader name,
c.tl_fusionid,
c.team_leader,
c.dept,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score",
case when coalesce(round(avg(a."Quality Score"),2),0) >= 95 then 7.5
when coalesce(round(avg(a."Quality Score"),2),0) >= 92.5 then 5
when coalesce(round(avg(a."Quality Score"),2),0) >= 90 then 2.5
else 0 end as "Quality_Score"
from
(
select 
a.month,
a.tl_fusionid "fusion_id",
a.supervisor_name,
c.tl_fusionid,
c.team_leader,
coalesce(round(avg(a."Quality Score"),2),0) "Quality Score"
from (
select last_day(roster_month) month,
a.fusion_id,c.ucid,a.employee_name,c.tl_fusionid,a.supervisor_name
,c.dept
,coalesce(round(avg(score),2),0) "Quality Score"
,rank() OVER (PARTITION by last_day(roster_month),c.dept ORDER BY coalesce(round(avg(score),2),0) desc,c.dept,last_day(roster_month) )  "Quality Rank"
,b."Out Of"
,case 
when coalesce(round(avg(score),2),0) >=97.50  then 3.75 
when coalesce(round(avg(score),2),0) >=95.20  then 2.5
when coalesce(round(avg(score),2),0) >=90  then 1.25 
else 0
end as
"Quality_Score"
from asit_quality a
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.roster_month)=c.month and a.fusion_id=c.fusionid
join(select last_day(month) monthn,dept,count(*) "Out Of"
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
group by month,dept
)b on last_day(a.roster_month)=b.monthn and b.dept=c.dept
--where a.roster_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
group by last_day(roster_month),a.fusion_id,c.ucid,a.employee_name,a.supervisor_name,c.dept,b."Out Of",c.tl_fusionid
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Team Lead')
and final_status='ACTIVE'
and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.supervisor_name,c.tl_fusionid,c.team_leader
) a 
join(
select month,fusionid,ucid,livevox_id,name,team_leader,location,dept,designation 
,tl_fusionid
from asit_roster_table
where month=last_day(month)
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Assistant Manager')
and final_status='ACTIVE'
--and team_leader not in (select name from not_teamleader)
)c on last_day(a.month)=c.month and a.tl_fusionid=c.fusionid
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept
)b on a.month=b.month and a.fusion_id=b."fusion_id"
where a."MONTH" >= last_day(period) and 
a."FUSION_ID" in (vfusionid5)
--and a."FUSION_ID"='100713'
order by a.month asc;    

  end if;
  END CR_SC_MTD_AM;