create table not_teamleader as select distinct fusionid,ucid,ntid,name,designation,team_leader,location,dept from asit_roster_table
where name like 'Thakur, Akshita'
and ntid is not null
--order by month desc

select * from NOT_TEAMLEADER



--select * from asit_ocr;
select a.monthn,a.dept,a."Out Of",a."Out of 95"
--,cast(coalesce(max(b.credits_per_hour),0) as decimal(10,3)) "Credit Per Hr"
,case when cast(a."Out of 95" as number)=
rank() OVER (PARTITION by last_day(b.rpt_month),a.dept ORDER BY coalesce(max(b.credits_per_hour),0) desc,a.dept,last_day(b.rpt_month) )
then cast(coalesce(max(b.credits_per_hour),0) as decimal(10,3)) end as "Target"
from (
select last_day(a.month) monthn,a.dept,count(*) "Out Of",round(count(*)*0.95) "Out of 95"
from asit_roster_table a
where a.month=last_day(a.month)
and a.dept in ('CCC-R','CCC-R SPANISH','FLEX')
and a.location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and a.designation in ('Agent')
and a.final_status='ACTIVE'
and a.team_leader not in (select name from not_teamleader)
group by last_day(a.month),a.dept
) a
join asit_res_credits  b on last_day(a.monthn)=last_day(b.rpt_month)-- and a.dept=b.dept
group by a.monthn,a.dept,a."Out Of",a."Out of 95",b.rpt_month



select month,fusionid,ucid,livevox_id,name
from asit_roster_table
where month>=last_day('01-Feb-24')
and dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')
and designation in ('Agent')
and final_status='ACTIVE'
group by month,fusionid,ucid,livevox_id,name
order by month
;

select * from asit_roster_table where month='29-FEB-24'



select * from asit_ocr where fusion_id='105554'
and month='31-JAN-24'



----------------------RES Credit-------------------------------------------
select * from asit_res_credits a where rpt_month='31-JAN-24'
and  dept in ('CCC-R','CCC-R SPANISH','FLEX')
and location in ('MUM','BAN','MTL','PMC','RC','STX','WPB')


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
where "CPH_TARGET" is not null) b on 
--where a.rpt_month=last_day('31-JAN-24') 
--and a.ucid in (select distinct ucid from asit_roster_table where fusionid='105554' ) 
order by a.dept


--------------------------------Quality------------------------------------------



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
order by c.dept


---------------------Stella Star rating----------------------------

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
order by a.employee_custom_id;

-------ACH Adherence-----------

--select adherence_percentage from asit_telephony;

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

where 
 a.month=last_day(a.month)
--and a.fusion_id in ('106158') 
--group by month,fusion_id
order by to_date(month),fusion_id,b."Out Of";


--------------------------AHT----------------

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
--where 

----------CMS Defect-----

--select * from asit_cms_defect;

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
where 
  a.month=last_day(a.month)
--and a.fusion_id in ('193222') 
--group by month,fusion_id
order by to_date(month),ucid;
----------------Target-------------
select * from CR_SC_AGNTS_TARGET



------Collated query MTD agents----------------------------

--select * from asit_roster_table where month='31-JAN-24'

select 
to_char(b."MONTH",'MON-YY') MONTH,
b."UCID",
b."EMPLOYEE ID",
b."NAME",
b."Team Lead",
b."LOCATION",
b."DEPT",
b."Overall Rank" "Global Rank",
b."Overall Score" "Total Points",
b."Credit Per Hr",
b."Credit Rank",
b."Credits Score",
b."CPH_TARGET",
b."Quality Score",
b."Quality Rank",
b."Quality_Score",
b."QUALITY_TARGET",
b."Stella Star Rating",
b."Stella Star Rank",
b."Stella Star Score",
b."STAR_TARGET",
b."Schedule Adherence",
b."Adherence Rank",
b."Adherence Score",
b."ADHERENCE_TARGET",
b."Inbound AHT",
b."AHT Rank",
b."AHT Score",
b."IB_AHT_TARGET",
b."Cms Defect %",
b."Cms Defect Rank",
b."Cms Defect Score",
b."CMS_DEFECT_TARGET",
b."Out Of",
b."Global Rank" "YTD Global Rank",
b."Global Total Points" "YTD Global Total Points"
from(
select 
a.month  ,a.ucid,a."EMPLOYEE ID",a.name,a."Team Lead",a.location,a.dept
,rank() OVER (PARTITION by a.month,a.dept ORDER BY
(coalesce(a."Credits Score",0)+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+coalesce(a."Adherence Score",0)+coalesce(a."AHT Score",0)
+coalesce(a."Cms Defect Score",0)) desc,a.dept,a.month ) "Overall Rank"
,(coalesce(a."Credits Score",0)+coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0)
+coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0)+coalesce(a."Adherence Score",0)+coalesce(a."AHT Score",0)
+coalesce(a."Cms Defect Score",0)) "Overall Score"
,a."Credit Per Hr",a."Credit Rank",a."Credits Score",a.CPH_TARGET
, a."Quality Score" 
,case when a."Quality Rank" is null then 1 else a."Quality Rank" end as "Quality Rank"
,coalesce(case when a."Quality_Score"is null then 3.75 else a."Quality_Score" end,0) as "Quality_Score",a.QUALITY_TARGET
,a."Stella Star Rating" 
,case when a."Stella Star Rank" is null then 1 else a."Stella Star Rank" end as "Stella Star Rank"
,coalesce(case when a."Stella Star Score" is null then 4 else a."Stella Star Score" end,0) as "Stella Star Score",a.STAR_TARGET
,a."Schedule Adherence",a."Adherence Rank",a."Adherence Score",a.ADHERENCE_TARGET
,a."Inbound AHT",a."AHT Rank",a."AHT Score",a.IB_AHT_TARGET
,a."Cms Defect %",a."Cms Defect Rank",a."Cms Defect Score",a.CMS_DEFECT_TARGET
,a."Out Of"
,b."Global Rank"
,b."Global Total Points"
from
(
select 
last_day(a.month) month,
a.ucid,
a.fusionid "EMPLOYEE ID",
a.name,
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
)a
left join (
select 
"EMPLOYEE ID"--,name,"Team Lead",location,dept
,rank() OVER (order by sum("Total Points") desc)  "Global Rank"
,sum("Total Points") "Global Total Points"
,avg("Credit Per Hr") "Credit Per Hr",
avg("Quality Score") "Quality Score",avg("Stella Star Rating") "Stella Star Rating",avg("Schedule Adherence") "Schedule Adherence",
avg("Inbound AHT") "Inbound AHT",avg("Cms Defect %") "Cms Defect %"
from
(
select 
to_char(a.month,'MON-YY') month ,a.ucid,a."EMPLOYEE ID",a.name,a."Team Lead",a.location,a.dept
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
--where  "EMPLOYEE ID" in('197233')
group by "EMPLOYEE ID"

) b on a."EMPLOYEE ID"=b."EMPLOYEE ID"
)b
--where b.month >= last_day('21-Feb-24')
where b."EMPLOYEE ID" in('103851')
order by b.month
--to_date(concat('01-',month)) asc
;










