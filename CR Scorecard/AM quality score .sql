--select * from asit_quality where roster_month='31-JAN-24'
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
group by a.month,a.tl_fusionid,a.team_leader,c.tl_fusionid,c.team_leader,c.dept;