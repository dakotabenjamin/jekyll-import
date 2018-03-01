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

        wgroup = <<EOS
          SELECT node.nid, fwg.field_working_group_nid AS wgid
          FROM field_data_field_working_group AS fwg, node
          WHERE fwg.entity_id = node.nid
EOS

        proj = <<EOS
          SELECT project.nid, GROUP_CONCAT(project.name SEPARATOR '|') as projects
          from (
               SELECT node.nid,
                 (SELECT node.title
                  FROM #{prefix}node
                  WHERE node.nid = proj.field_update_project_nid) AS name
                FROM #{prefix}field_data_field_update_project AS proj, node
                WHERE proj.entity_id = node.nid
               ) as project
          GROUP BY project.nid
EOS

        query = <<EOS
                SELECT n.nid,
                       n.title,
                       fdb.body_value,
                       fdb.body_summary,
                       n.created,
                       n.status,
                       n.type,
                       users.name,
                       project.projects,
                       (select title from node where node.nid = wg.wgid) as wgroup,
                       #{tag_group}
                FROM #{prefix}node AS n
                LEFT JOIN #{prefix}field_data_body AS fdb
                  ON fdb.entity_id = n.nid AND fdb.entity_type = 'node'
                LEFT JOIN #{prefix}users AS users
                  ON users.uid = n.uid
                LEFT JOIN (#{wgroup}) as wg
                  ON n.nid = wg.nid
                LEFT JOIN (#{proj}) as project
                  ON project.nid = n.nid
                WHERE (#{types})
EOS

        return query
      end

      def self.aliases_query(prefix)
        "SELECT source, alias FROM #{prefix}url_alias WHERE source = ?"
      end

      def self.post_data(sql_post_data)
        content = sql_post_data[:body_value].to_s
        summary = sql_post_data[:body_summary].to_s
        tags = (sql_post_data[:tags] || "").downcase.strip
        time = sql_post_data[:created]
        wg = (sql_post_data[:wgroup] || "")
        projects = (sql_post_data[:projects] || "")

        data = {
          "Summary Text"    => summary,
          "Person"          => sql_post_data[:name],
          "date"            => Time.at(time).to_datetime.strftime("%Y-%m-%d %H:%M:%S Z").to_s,
          "Working Group"   => wg.split("|"),
          "Projects"        => projects.split("|")

        }

        return data, content
      end
    end
  end
end
