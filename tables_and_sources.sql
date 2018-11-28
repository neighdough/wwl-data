select distinct field_desc, 
	case
    	when categoryid = '1'
        	then 'Demographics'
		when categoryid = '2'
        	then 'Community Development'
        when categoryid = '3'
        	then 'Housing'
        when categoryid = '4'
        	then 'Transportation'
        when categoryid = '5'
        	then 'Environment'
        when categoryid = '6'
        	then 'Economy & Jobs'
        when categoryid = '7'
        	then 'Education'
        when categoryid = '8'
        	then 'Arts & Community'
		when categoryid = '9'
        	then 'Community Engagement'
        when categoryid = '10'
        	then 'Health & Safety'
	end as category,
	title, 
    source, 
    case 
        when source similar to
            '%(U.S. Green Building Council'
            '|Federal Communications Commission; 08/19/2016'
            '|Memphis Light, Gas, and Water'
            '|Memphis Urban Area Metropolitan Planning Organization)%'
        then 'Winter'
        when source similar to
            '%(U.S. Environmental Protection Agency)%'
        then 'Spring'
        when source similar to
            '%(Shelby County Assessor''s Certified Roll'
            '|U.S. Census Bureau; American Community Survey)%'
        then 'Summer'
        when source similar to
            '%(Memphis Area Transit Authority'
            '|National Highway Traffic Safety Administration '
            'Fatality Analysis Reporting System)%'
        then 'Fall'
        else 'None'
        end
    as update_period,
    array_to_string(geography, ', ') geography,   
    case
    	when column_name = any (array(select field from data_dictionary_chng)) 
        then 'Yes'
    	  else 'No'
	end as change
from information_schema.columns
left join (select column_name c, 
  array_agg(distinct split_part(table_name, '_', 3)) geography
		from information_schema.columns
		where split_part(table_name, '_', 1) = 'summary'
 		group by column_name) agg on c = column_name
left join (select field, field_desc, source, title, categoryid 
      from data_dictionary, data_sources 
      where descid = citation) dict on field = column_name
where split_part(table_name, '_', 1) = 'summary' and field_desc is not null
	-- and agg.column_name = col.column_name
order by category, field_desc -- source, field_desc
