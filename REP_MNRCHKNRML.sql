PROCEDURE REP_MNRCHKNRML IS
  fh TEXT_IO.FILE_TYPE;
	vlstpath VARCHAR2(100);
	v_cnt number := 1;
	v_status varchar2(50);
	v_equpiment varchar2(100);
	v_element varchar2(100);
cursor mnrchknrml is
select 
	substr(mn.mncw_mneq_code, 1, 3) shop,
	mn.mncw_mneq_code equipment_code,
	mn.mncw_hth_chk1 health_condition1,
	mn.mncw_hth_chk2 health_condition2,
	mn.mncw_mnec_chkpoint_code element,
	mn.mncw_chk1_freq1 motor1,
	mn.mncw_chk1_freq2 driven_equipment1,
	mncw_chk1_freq3,
	mncw_chk1_freq4,
	mncw_chk1_freq5,
	mncw_chk1_freq6,
	/*MATMQL-13214*/
	mn.mncw_chk2_freq1 motor2,
	mn.mncw_chk2_freq2 driven_equipment2,
	mncw_chk2_freq3,
	mncw_chk2_freq4,
	mncw_chk2_freq5,
	mncw_chk2_freq6,
	mn.mncw_analysis analysis,
	MNCW_COUNTERMEASURE1 counter_measure,
	MNCW_START_TIME checking_date1,
	mn.mncw_close_date checking_date2,
	mn.mncw_hth_chk2 present_status,
	mn.mncw_remarks remarks,
	mn.mncw_end_time   ,
	mn.mncw_man_hours hours,
	mn.mncw_work_done_details  workdone,
	mn. mncw_work_order_no,
	substr(mn.mncw_mnef_mncm_code,5,1) mncm_code
from mat.mmat_mncw mn
where --mn.mncw_done_on>= TO_CHAR(:FROM_DATE,'DD-MON-RRRR')
--AND mn.mncw_done_on <= TO_CHAR(:TO_DATE,'DD-MON-RRRR')
--and mn.MNCW_STATUS ='SNC'
and mn.mncw_group like decode(:BLK_CTRL.DR_GROUP, 'ALL', mncw_group, :BLK_CTRL.DR_GROUP)
and substr(mn.mncw_mneq_code, 1, 3) in
	--(select distinct dp.MNDP_CODE
	--from mat.mmat_mndp dp
	--where dp.mndp_comp_code = :global.company
	and trunc(sysdate) between fate_col1 and nvl(date_col2, trunc(sysdate) + 1)
	--and dp.MNDP_DEPT_CODE like decode(:BLK_CTRL.DEPARTMENT, 'ALL', '%', :BLK_CTRL.DEPARTMENT)
	--and dp.mndp_code like decode(:BLK_CTRL.SECTION, 'ALL', '%', :BLK_CTRL.SECTION)
	)
--and mn.mncw_comp_code = :global.company
--and substr(mn.mncw_mnef_mncm_code,5,1) like decode (:BLK_CTRL.CHK_STATUS,'ALL','%',:BLK_CTRL.CHK_STATUS);

Begin

	IF :BLK_CTRL.CHK_STATUS ='V' THEN
  	v_status := 'VIBRATION ANALYSIS';
  ELSIF :BLK_CTRL.CHK_STATUS ='O' THEN
  	v_status := 'OIL ANALYSIS';
  ELSIF :BLK_CTRL.CHK_STATUS ='T' THEN
  	v_status := 'THERMOGRAPHY';
  ELSIF :BLK_CTRL.CHK_STATUS ='ALL' THEN
  	v_status := 'ALL';
  ELSE
  	v_status := ' ';
  END IF;
  

Tool_Env.Getvar('MATLSTPATH',vlstpath);
:global.rep_name := 'mnrchknrml_'||user||'.xls';
fh := TEXT_IO.FOPEN (vlstpath||'/'||:global.rep_name, 'W');
TEXT_IO.PUT_LINE (fh,' '||chr(9)||' '||V_STATUS||' '||chr(9)||'Maruti Suzuki India Ltd.');
TEXT_IO.PUT_LINE (fh,' '||chr(9)||' '||'Group'||chr(9)||:BLK_CTRL.DR_GROUP||chr(9)||'RECHECK OK DETAIL REPORT');
TEXT_IO.PUT_LINE (fh,' '||chr(9)||' '||'Department'||chr(9)||:BLK_CTRL.DEPARTMENT||chr(9)||'From'||chr(9)||TO_CHAR(:FROM_DATE,'DD-MON-RRRR')||
				  chr(9)||'To'||TO_CHAR(:TO_DATE,'DD-MON-RRRR'));
TEXT_IO.PUT_LINE (fh,' '||chr(9)||' '||'Section'||chr(9)||:BLK_CTRL.SECTION||chr(9)||
				 'Vibration Standard (Values in mm/sec) 0-4.4-    Normal   4.5-11.1-  Alert  >11.2- Alarm'
				 ||chr(9)||'Run Date'||chr(9)||sysdate);                          
TEXT_IO.PUT_LINE (fh,
'Sno '||chr(9)||
'Work Order No. '||chr(9)||
'Shop '||chr(9)||
'Equipment Code '||chr(9)||
'Equipment Name '||chr(9)||
'Element Code '  ||chr(9)||
'Element Name '||chr(9)||
'Motor/JT-1 '||chr(9)||
'Driven Equipment/JT-2 '||chr(9)||
'JT-3'||
CHR(9)||
'JT-4'||
CHR(9)||
'JT-5'||
CHR(9)||
'JT-6'||
CHR(9)||
'Health Condition '||
chr(9)||
'Checking Date '||
chr(9)||
'Motor/JT-1 '||
chr(9)||
'Driven Equipment/JT-2 '||
chr(9)||
'JT-3'||
CHR(9)||
'JT-4'||
CHR(9)||
'JT-5'||
CHR(9)||
'JT-6'||
chr(9)||
'Health Condition '||
chr(9)||
'Checking Date '||
chr(9)||
'Work Done Detail '
||
chr(9)||
'Action Taken Date '||
chr(9)||
'Man Hours '||
chr(9)||
'Remarks ');

  			for i in mnrchknrml loop

						begin
							select MNEQ_DESC into v_equpiment from mat.mmat_mneq
							where MNEQ_COMP_CODE =:GLOBAL.COMPANY
							AND MNEQ_CODE = I.EQUIPMENT_CODE;
						exception
							when others then
							v_equpiment := null;
						end;

	  			begin
		  				SELECT MNEC_CHKPOINT_DESC INTO V_ELEMENT FROM MAT.MMAT_MNEC
						   WHERE MNEC_COMP_CODE=:GLOBAL.COMPANY
			           AND MNEC_MNEQ_CODE=I.EQUIPMENT_CODE
			           AND MNEC_CHKPOINT_CODE=I.ELEMENT ;
					exception
						when others then
						v_element := null;
					end;

TEXT_IO.PUT_LINE (fh,v_cnt||chr(9)||
i.MNCW_WORK_ORDER_NO||chr(9)||
i.SHOP||chr(9)||
i.EQUIPMENT_CODE||chr(9)||
replace(replace(v_equpiment,chr(10),chr(32)),chr(9),chr(32))||chr(9)||
i.ELEMENT||chr(9)||
replace(replace(v_element,chr(10),chr(32)),chr(9),chr(32))||chr(9)||
i.motor1||chr(9)||
i.driven_equipment1||chr(9)||
i.mncw_chk1_freq3||chr(9)||
i.mncw_chk1_freq4||chr(9)||
i.mncw_chk1_freq5||chr(9)||
i.mncw_chk1_freq6||chr(9)||
i.health_condition1||chr(9)||
to_char(i.checking_date1,'DD-MON-RRRR')||chr(9)||
i.motor2||chr(9)||
i.driven_equipment2||chr(9)||
i.mncw_chk2_freq3||chr(9)||
i.mncw_chk2_freq4||chr(9)||
i.mncw_chk2_freq5||chr(9)||
i.mncw_chk2_freq5||chr(9)||
i.health_condition2||chr(9)||
to_char(i.checking_date2,'DD-MON-RRRR')||chr(9)||
i.workdone||chr(9)||
i.mncw_end_time||chr(9)||
i.hours||chr(9)||
i.remarks

);	
	v_cnt := v_cnt + 1;
	
  			end loop;	
  			
  			TEXT_IO.FCLOSE(fh);
				call_form('$FORMSPATH/print_email');

EXCEPTION WHEN OTHERS THEN 
	        Message('Error in excel generation : '||SQLERRM);
End;
