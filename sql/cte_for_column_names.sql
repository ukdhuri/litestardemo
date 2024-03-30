with final_result as
(
    {{ base_select_query|safe }}
) 
select * from final_result
where 1=0
