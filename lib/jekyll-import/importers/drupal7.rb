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
                select c.name, c.official_name, c.iso2, c.iso3, c.continent from countries_country c
EOS

        return query
      end

      def self.aliases_query(prefix)
        "SELECT source, alias FROM #{prefix}url_alias WHERE source = ?"
      end

      def self.post_data(sql_post_data)

        cname = sql_post_data[:name].to_s
        official_name = sql_post_data[:official_name].to_s
        content = ''
        iso2 = sql_post_data[:iso2].to_s
        iso3 = sql_post_data[:iso3].to_s
        continent = sql_post_data[:continent].to_s
        url_name = cname.downcase.tr(".,()","").tr(" ", "-")

        data = {
          "title"     => cname,
          "continent" => continent,
          "urlname"   => url_name,
          "names"     => [cname, official_name, iso2, iso3]
        }

        return data, content
      end
    end
  end
end
