 {#
    This macro returns the Name of company
    unfortunately column is dropped since its not filled with any, but made with given data for future iterations  
#}

{% macro get_company_Name(hvfhs_license_num) -%}

    case {{ hvfhs_license_num }}
        when 'HV0002' then 'Juno'
        when 'HV0003' then 'Uber'
        when 'HV0004' then 'Via'
        when 'HV0005' then 'Lyft'
    end

{%- endmacro %}

{% macro formatted_date(execution_date) %}

  {% set execution_date_obj = modules.datetime.datetime.strptime(execution_date|string, "%Y%m%d") %}
  {{ return(execution_date_obj.strftime("%Y-%m")) }}

{% endmacro %}