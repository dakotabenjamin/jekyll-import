require "jekyll-import/importers/drupal_common"

module JekyllImport
  module Importers
    class Drupal7 < Importer
      include DrupalCommon
      extend DrupalCommon::ClassMethods

      def self.build_query(prefix, types, engine)
        types = types.join("' OR n.type = '")
        types = "n.type = '#{types}'"

        if engine == "postgresql"
          tag_group = <<EOS
            (SELECT STRING_AGG(td.name, '|')
            FROM taxonomy_term_data td, taxonomy_index ti
            WHERE ti.tid = td.tid AND ti.nid = n.nid) AS tags
EOS
        else
          tag_group = <<EOS
            (SELECT GROUP_CONCAT(td.name SEPARATOR '|')
            FROM taxonomy_term_data td, taxonomy_index ti
            WHERE ti.tid = td.tid AND ti.nid = n.nid) AS 'tags'
EOS
        end

        query = <<EOS
        
          select n.nid,
                 n.title,
                 n.created,
                 fdb.body_value,
                 fdb.body_summary,
                 (select node.title from node where fdpartners.field_partners_nid = node.nid) as partners,
                 coordinators.coords as coordinators,
                 fduration.field_timespan_value as startdate,
                 fduration.field_timespan_value2 as enddate,
                 fdlink.field_link_url as url,
                 ptype.field_project_type_tid as typeid,
                 contact.field_contact_value as contact
          from node as n
          LEFT JOIN field_data_body AS fdb
            ON fdb.entity_id = n.nid AND fdb.entity_type = 'node' AND fdb.bundle = 'project'
          left join (
                    select projects.field_projects_nid as nid2, GROUP_CONCAT(coord.field_name_value SEPARATOR '|') as coords
                    from field_data_field_name as coord
                    left join field_data_field_projects as projects
                      on projects.entity_id = coord.entity_id
                    group by nid2
          ) as coordinators
            on coordinators.nid2 = n.nid
          left join field_data_field_partners as fdpartners
            on fdpartners.entity_id = n.nid
          left join field_data_field_timespan as fduration
            on fduration.entity_id = n.nid
          LEFT JOIN field_data_field_link as fdlink
            on fdlink.entity_id = n.nid
          LEFT JOIN field_data_field_project_type as ptype
            on ptype.entity_id = n.nid
          LEFT JOIN field_data_field_contact as contact
            ON contact.entity_id = n.nid
          where n.type = 'project'
EOS

        return query
      end

      def self.aliases_query(prefix)
        "SELECT source, alias FROM #{prefix}url_alias WHERE source = ?"
      end

      def self.post_data(sql_post_data)
        content = sql_post_data[:body_value].to_s
        summary = sql_post_data[:body_summary].to_s
        # tags = (sql_post_data[:tags] || "").downcase.strip
        time = Time.at(sql_post_data[:created]).to_datetime.strftime("%Y-%m-%d %H:%M:%S Z").to_s
        coordinators = sql_post_data[:coordinators].to_s
        startdate = sql_post_data[:startdate].to_s
        enddate = sql_post_data[:enddate].to_s
        ongoing = (startdate == enddate)
        link = sql_post_data[:url].to_s
        types= {
            "218" => "Disaster Mapping",
            "219" => "Community Development",
            "220" => "Technical Projects",
            "234" => "Partnerships",
        }
        tid = sql_post_data[:typeid].to_s

        data = {
            "date"            => time,
            "Summary Text"    => summary,
            "HOT Involvement" => types[tid],
            "Person"          => coordinators.split('|'),
            "Partner"         => sql_post_data[:partners].to_s,
            "Link"            => link,
            "Duration"        => {"Start Date"  => startdate, "End Date" => ongoing ? nil : enddate},
            "permalink"       => sql_post_data[:title].to_s.gsub(' - ', ' ').tr(':,|.()','').downcase.gsub(' ', '_'),
            "Contact"         => sql_post_data[:contact].to_s,
        }
        return data, content
      end
    end
  end
end
