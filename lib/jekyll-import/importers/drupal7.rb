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
                SELECT u.uid,
                        u.name as user,
                        fdname.field_name_value as name,
                        fdbio.field_bio_value as bio,
                        fdprojects.projects,
                        u.created,
                        u.status,
                        staff.field_hot_staff_type_tid as staff_id,
                        (select c.name 
                          from countries_country c 
                          where countries.field_country_iso2 = c.iso2 limit 1) 
                            as country,
                        workinggroups.wgroups,
                        osm.field_osm_user_name_value as osmname,
                        linkedin.field_linkedin_url as linkedin_url,
                        fb.field_facebook_url as fb_url,
                        twitter.field_twitter_value as twitter_username,
                        volunteers.field_volunteer_lead_role_value,
                        (select t.name
                          from taxonomy_term_data t
                          where hot_leadership.field_hot_leadership_tid = t.tid limit 1)
                            as hot_leadership_role,
                         (SELECT f.filename from file_managed f where f.fid = u.picture) as picture
                FROM users AS u
                LEFT JOIN field_data_field_name AS fdname
                  ON fdname.entity_id = u.uid AND fdname.entity_type = 'user'
                LEFT JOIN field_data_field_bio as fdbio
                  on fdbio.entity_id = u.uid AND fdbio.entity_type = 'user'
                LEFT JOIN field_data_field_country as countries
                  on countries.entity_id = u.uid
                LEFT JOIN
                  ( select u.uid, GROUP_CONCAT(fdp.title SEPARATOR '|') as projects
                    from (SELECT n.nid, n.title, fp.entity_id, fp.entity_type 
                          from node n, field_data_field_projects fp 
                          where n.nid = fp.field_projects_nid) 
                          as fdp
                    left join users as u on u.uid = fdp.entity_id AND fdp.entity_type = 'user' group by u.uid) as fdprojects
                      on fdprojects.uid = u.uid
                LEFT JOIN field_data_field_hot_staff_type as staff
                  on staff.entity_id = u.uid
                left join (select u.uid, GROUP_CONCAT((select n.title from node n where fdwg.field_working_group_nid = n.nid limit 1) SEPARATOR "|") as wgroups
                           from field_data_field_working_group fdwg
                           left join users as u on u.uid = fdwg.entity_id and fdwg.entity_type = 'user' group by u.uid) as workinggroups
                    on workinggroups.uid = u.uid
                left join field_data_field_osm_user_name as osm
                  on osm.entity_id = u.uid
                left join field_data_field_linkedin as linkedin
                  on linkedin.entity_id = u.uid
                left join field_data_field_facebook as fb
                  on fb.entity_id = u.uid
                left join field_data_field_twitter as twitter
                  on twitter.entity_id = u.uid
                left join field_data_field_volunteer_lead_role as volunteers
                  on volunteers.entity_id = u.uid
                left join field_data_field_hot_leadership as hot_leadership
                  on hot_leadership.entity_id = u.uid
EOS

        return query
      end

      def self.aliases_query(prefix)
        "SELECT source, alias FROM #{prefix}url_alias WHERE source = ?"
      end

      def self.post_data(sql_post_data)

        staff_types = {
            "221" => "Executive Director",
            "222" => "Employee",
            "223" => "Contractor"
        }

        content = sql_post_data[:bio].to_s
        uname = sql_post_data[:name].to_s
        staff = sql_post_data[:staff_id]
        workinggroups = sql_post_data[:wgroups].to_s.split('|')
        projects = sql_post_data[:projects].to_s.split('|')
        country = sql_post_data[:country].to_s
        osm = sql_post_data[:osm]
        twitter = sql_post_data[:twitter_username]
        socialmedia = {
            "OSM"       => osm.nil? ? nil : "https://www.openstreetmap.org/user/" + osm.to_s,
            "LinkedIn"  => sql_post_data[:linkedin_url].to_s,
            "Facebook"  => sql_post_data[:fb_url].to_s,
            "Twitter"   => twitter.nil? ? nil : "https://twitter.com/" + twitter.to_s
        }.delete_if { |_k, v| v.nil? || v == "" || v.to_s == "[]" }
        member_type = {
          "Is Staff"          => staff.nil? ? nil : true,
          "Is Voting Member"  => nil,
          "Is Board Member"   => sql_post_data[:hot_leadership_role].nil? ? nil : true,
        }.delete_if { |_k, v| v.nil? || v == "" }
        photo = sql_post_data[:picture].nil? ? nil : "/uploads/" + sql_post_data[:picture].to_s


        data = {
          "title"                   => uname,
          "Working Group"           => workinggroups,
          "Project"                 => projects,
          "Country"                 => country,
          "Social Media (Full URL)" => socialmedia,
          "Member Type"             => member_type,
          "permalink"               => 'users/' + uname.tr(' ', '_').downcase,
          "Photo"                   => photo,
        }

        return data, content
      end
    end
  end
end
