#!/usr/bin/env bash

#for t in Sales_Performance_Goals Sales_Reporting_Calendar sf_Account  sf_AccountHistory sf_Account_Project_Relation_c sf_Calendar_Year_c sf_Campaign sf_CampaignMember sf_CampaignMemberStatus sf_Change_Request_c sf_Contact sf_Daily_Time_Entry_c sf_Lead sf_LeadCleanInfo sf_LeadFeed sf_LeadHistory sf_LeadStatus sf_Opportunity sf_OpportunityFieldHistory sf_OpportunityHistory sf_Project_Invoice_Resource_c sf_Project_Invoice_c  sf_Project_Resource_Hours_History sf_Project_Resource_Hours_c sf_Project_c  sf_Resource_Profile_c sf_Staffing_Plan_c sf_Task sf_TimeLive_Staging_Share sf_TimeLive_Staging_c sf_User time_history

for t in sf_Account  sf_AccountHistory

do
    echo "replicating $t"
    curl  -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
          -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5"  \
         "https://processor.analytics.tectonic-cloud.com/Replicate?object-name=$t&source-project=tectonic-analytics&source-dataset=tectonic_commix&dest-project=tt-cust-analytics&dest-dataset=RepTest&disposition=WRITE_TRUNCATE"
    echo
done