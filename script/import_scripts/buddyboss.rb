# frozen_string_literal: true

require 'mysql2'
require 'json'
require File.expand_path(File.dirname(__FILE__) + '/php_serialize')

require File.expand_path(File.dirname(__FILE__) + "/base.rb")

class ImportScripts::BuddyBoss < ImportScripts::Base

  BBOSS_HOST            ||= ENV['BBOSS_HOST'] || "127.0.0.1"
  BBOSS_DB              ||= ENV['BBOSS_DB'] || "local"
  BATCH_SIZE               ||= 1000
  BBOSS_PW              ||= ENV['BBOSS_PW'] || "root"
  BBOSS_USER            ||= ENV['BBOSS_USER'] || "root"
  BBOSS_PREFIX          ||= ENV['BBOSS_PREFIX'] || "wp_"
  BBOSS_PORT            ||= ENV['BBOSS_PORT'] || 10004
  BBOSS_ATTACHMENTS_DIR ||= ENV['BBOSS_ATTACHMENTS_DIR'] || "/Users/cwhatley/Local Sites/epsilontheorycom/app/public/wp-content/uploads/"
  POSTS_JSON            ||= ENV['POSTS_JSON'] || "/Users/cwhatley/2fp/et-discourse-migration/excerpt-dump.jsonl"
  NOTES_CATEGORY_ID ||= 47003

  def initialize
    super

    @he = HTMLEntities.new

    @posts_excerpts = {}

    @client = Mysql2::Client.new(
      host: BBOSS_HOST,
      username: BBOSS_USER,
      database: BBOSS_DB,
      port: BBOSS_PORT,
      password: BBOSS_PW,
    )
  end

  def execute
    apply_site_settings
    load_posts_json
    import_users
    import_anonymous_users
    import_categories
    import_topics_and_posts
    import_private_messages
    import_attachments
    create_permalinks
    import_comment_users
    import_post_topics
    import_post_comments
  end

  def apply_site_settings
    sql = <<-SQL
        update site_settings set value = :value, updated_at = now() where name = :name
    SQL
    DB.exec(sql, name: 'send_welcome_message', value: 'f') rescue puts "setting fail"
    DB.exec(sql, name: 'min_first_post_length', value: '10') rescue puts "setting fail2"
  end

  def load_posts_json
    File.open(POSTS_JSON).each do |line|
      rec = JSON.parse(line)
      @posts_excerpts[rec['id']] = rec
    end
    puts "loaded excerpts #{@posts_excerpts.length}"
  end

  def import_users
    puts "", "importing users..."

    last_user_id = -1
    total_users = bbpress_query(<<-SQL
      SELECT COUNT(DISTINCT(u.id)) AS cnt
      FROM #{BBOSS_PREFIX}users u
      LEFT JOIN #{BBOSS_PREFIX}posts p ON p.post_author = u.id
      WHERE p.post_type IN ('forum', 'reply', 'topic')
        AND user_email LIKE '%@%'
    SQL
    ).first["cnt"]

    batches(BATCH_SIZE) do |offset|
      users = bbpress_query(<<-SQL
        SELECT u.id, user_nicename, display_name, user_email, user_registered, user_url, user_pass
          FROM #{BBOSS_PREFIX}users u
          LEFT JOIN #{BBOSS_PREFIX}posts p ON p.post_author = u.id
         WHERE user_email LIKE '%@%'
           AND p.post_type IN ('forum', 'reply', 'topic')
           AND u.id > #{last_user_id}
      GROUP BY u.id
      ORDER BY u.id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if users.empty?

      last_user_id = users[-1]["id"]
      user_ids = users.map { |u| u["id"].to_i }

      next if all_records_exist?(:users, user_ids)

      user_ids_sql = user_ids.join(",")

      users_description = {}
      bbpress_query(<<-SQL
        SELECT user_id, meta_value description
          FROM #{BBOSS_PREFIX}usermeta
         WHERE user_id IN (#{user_ids_sql})
           AND meta_key = 'description'
      SQL
      ).each { |um| users_description[um["user_id"]] = um["description"] }

      users_last_activity = {}
      bbpress_query(<<-SQL
        SELECT user_id, meta_value last_activity
          FROM #{BBOSS_PREFIX}usermeta
         WHERE user_id IN (#{user_ids_sql})
           AND meta_key = 'last_activity'
      SQL
      ).each { |um| users_last_activity[um["user_id"]] = um["last_activity"] }

      create_users(users, total: total_users, offset: offset) do |u|
        {
          id: u["id"].to_i,
          username: u["user_nicename"],
          password: u["user_pass"],
          email: u["user_email"].downcase,
          name: u["display_name"].presence || u['user_nicename'],
          created_at: u["user_registered"],
          website: u["user_url"],
          bio_raw: users_description[u["id"]],
          last_seen_at: users_last_activity[u["id"]],
        }
      end
    end
  end

  def import_anonymous_users
    puts "", "importing anonymous users..."

    anon_posts = Hash.new
    anon_names = Hash.new
    emails = Array.new

    # gather anonymous users via postmeta table
    bbpress_query(<<-SQL
      SELECT post_id, meta_key, meta_value
        FROM #{BBOSS_PREFIX}postmeta
       WHERE meta_key LIKE '_bbp_anonymous%'
    SQL
    ).each do |pm|
      anon_posts[pm['post_id']] = Hash.new if not anon_posts[pm['post_id']]

      if pm['meta_key'] == '_bbp_anonymous_email'
        anon_posts[pm['post_id']]['email'] = pm['meta_value']
      end
      if pm['meta_key'] == '_bbp_anonymous_name'
        anon_posts[pm['post_id']]['name'] = pm['meta_value']
      end
      if pm['meta_key'] == '_bbp_anonymous_website'
        anon_posts[pm['post_id']]['website'] = pm['meta_value']
      end
    end

    # gather every existent username
    anon_posts.each do |id, post|
      anon_names[post['name']] = Hash.new if not anon_names[post['name']]
      # overwriting email address, one user can only use one email address
      anon_names[post['name']]['email'] = post['email']
      anon_names[post['name']]['website'] = post['website'] if post['website'] != ''
    end

    # make sure every user name has a unique email address
    anon_names.each do |k, name|
      if not emails.include? name['email']
        emails.push ( name['email'])
      else
        name['email'] = "anonymous_#{SecureRandom.hex}@no-email.invalid"
      end
    end

    create_users(anon_names) do |k, n|
      {
        id: k,
        email: n["email"].downcase,
        name: k,
        website: n["website"]
      }
    end
  end

  def import_categories
    puts "", "importing categories..."

    categories = bbpress_query(<<-SQL
      SELECT id, post_name, post_parent, post_title
        FROM #{BBOSS_PREFIX}posts
       WHERE post_type = 'forum'
         AND LENGTH(COALESCE(post_name, '')) > 0
    ORDER BY post_parent, id
    SQL
    )

    create_categories(categories) do |c|
      category = { id: c['id'], slug: c['post_name'], name: c['post_title'] || c['post_name'] }
      if (parent_id = c['post_parent'].to_i) > 0
        category[:parent_category_id] = category_id_from_imported_category_id(parent_id)
      end
      category
    end
  end

  def import_topics_and_posts
    puts "", "importing topics and posts..."

    # maps the mysql topic number to a counter with current post number
    topic_post_number_counters = {};

    last_post_id = -1
    total_posts = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BBOSS_PREFIX}posts
       WHERE post_status <> 'spam'
         AND post_type IN ('topic', 'reply')
    SQL
                               ).first["count"]

    # note ulike meta group is topic for topics and replies

    batches(BATCH_SIZE) do |offset|
      posts = bbpress_query(<<-SQL
        SELECT p.id id,
               p.post_author post_author,
               p.post_date post_date,
               p.post_content post_content,
               p.post_title post_title,
               p.post_type post_type,
               p.post_parent topic_id,
               pp.post_author as parent_post_author,
               coalesce(pm.meta_value, p.post_parent) as post_parent,
               ul.meta_value as likers_list
          FROM #{BBOSS_PREFIX}posts as p
          LEFT OUTER JOIN #{BBOSS_PREFIX}posts as pp on pp.id = p.post_parent
          LEFT OUTER JOIN #{BBOSS_PREFIX}postmeta as pm on p.id = pm.post_id and pm.meta_key = '_bbp_reply_to'
          LEFT OUTER JOIN #{BBOSS_PREFIX}ulike_meta as ul on p.id = ul.item_id
                          and ul.meta_key = 'likers_list' and ul.meta_group = 'topic'
         WHERE p.post_status <> 'spam'
           AND p.post_type IN ('topic', 'reply')
           AND p.id > #{last_post_id}
      ORDER BY p.id ASC
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if posts.empty?

      last_post_id = posts[-1]["id"].to_i
      post_ids = posts.map { |p| p["id"].to_i }

      next if all_records_exist?(:posts, post_ids)

      post_ids_sql = post_ids.join(",")

      # posts_likes = {}
      # bbpress_query(<<-SQL
      #   SELECT post_id, meta_value likes
      #     FROM #{BBOSS_PREFIX}postmeta
      #    WHERE post_id IN (#{post_ids_sql})
      #      AND meta_key = 'Likes'
      # SQL
      # ).each { |pm| posts_likes[pm["post_id"]] = pm["likes"].to_i }

      anon_names = {}
      bbpress_query(<<-SQL
        SELECT post_id, meta_value
          FROM #{BBOSS_PREFIX}postmeta
         WHERE post_id IN (#{post_ids_sql})
           AND meta_key = '_bbp_anonymous_name'
      SQL
      ).each { |pm| anon_names[pm["post_id"]] = pm["meta_value"] }

      create_posts(posts, total: total_posts, offset: offset) do |p|
        skip = false

        user_id = user_id_from_imported_user_id(p["post_author"]) ||
                  find_user_by_import_id(p["post_author"]).try(:id) ||
                  user_id_from_imported_user_id(anon_names[p['id']]) ||
                  find_user_by_import_id(anon_names[p['id']]).try(:id) ||
                  -1

        liker_ids = []
        if p["likers_list"]
          liker_import_ids = PHP.unserialize(p["likers_list"]){ |id| id.to_i }
          liker_ids = liker_import_ids.map{ |id| user_id_from_imported_user_id(id) ||
                                            find_user_by_import_id(id).try(:id) || -1 }.select{ |id| id != -1 }
        end

        post = {
          id: p["id"],
          user_id: user_id,
          raw: p["post_content"],
          created_at: p["post_date"],
          post_create_action: proc do |new_post|
            create_post_like_actions(liker_ids, new_post[:topic_id], new_post[:id], user_id, new_post[:created_at])
            new_post[:like_score] = new_post[:like_count] = liker_ids.length
            new_post.save
          end
        }

        if post[:raw].present?
          post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
          post[:raw].gsub!(/\<img src="image\/png.*$/im) { "" }
        end
        # TODO - the threading still doesn't work here or for comments
        if p["post_type"] == "topic"
          post[:category] = category_id_from_imported_category_id(p["post_parent"])
          post[:title] = CGI.unescapeHTML(p["post_title"])
        else
          if parent = topic_lookup_from_imported_post_id(p["post_parent"])
            post[:topic_id] = parent[:topic_id]
            # TODO: wtf is up with post number
            if !topic_post_number_counters[parent[:topic_id]]
              topic_post_number_counters[parent[:topic_id]] = parent[:post_number]
            end
            topic_post_number = topic_post_number_counters[parent[:topic_id]] += 1
            parent_post_id = post_id_from_imported_post_id(p["post_parent"])
            post[:post_number] = topic_post_number
            post[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
          else
            puts "Skipping #{p["id"]}: #{p["post_content"][0..40]}"
            skip = true
          end
        end

        skip ? nil : post
      end
    end
  end

  def import_attachments
    import_attachments_from_postmeta
  end

  def import_attachments_from_postmeta
    puts "", "Importing attachments from 'postmeta'..."

    count = 0
    last_attachment_id = -1


    total_attachments = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BBOSS_PREFIX}bp_media pm
    SQL
    ).first["count"]

    batches(BATCH_SIZE) do |offset|
      attachments = bbpress_query(<<-SQL
       SELECT pm.meta_id id, pm.meta_value media_ids, pm.post_id post_id, apm.meta_value as filename
          FROM #{BBOSS_PREFIX}postmeta pm
          JOIN #{BBOSS_PREFIX}bp_media med ON pm.meta_value REGEXP concat('^([0-9]+,)*', med.id , '(,[0-9]+)*$')
          JOIN #{BBOSS_PREFIX}posts p ON p.id = med.attachment_id
          JOIN #{BBOSS_PREFIX}postmeta apm on apm.post_id = med.attachment_id and apm.meta_key = '_wp_attached_file'
         WHERE pm.meta_key = 'bp_media_ids'
           AND pm.meta_id > #{last_attachment_id}
       ORDER BY pm.meta_id, med.id
       LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if attachments.empty?
      last_attachment_id = attachments[-1]["id"].to_i

      attachments.each do |a|
        print_status(count += 1, total_attachments, get_start_time("attachments_from_postmeta"))
        path = File.join(BBOSS_ATTACHMENTS_DIR, a["filename"])
        if File.exists?(path)
          if post = Post.find_by(id: post_id_from_imported_post_id(a["post_id"]))
            filename = File.basename(a["filename"])
            upload = create_upload(post.user.id, path, filename)
            if upload&.persisted?
              html = html_for_upload(upload, filename)
              if !post.raw[html]
                post[:raw].gsub!(/\<img src="image\/png.*$/im) { "" }
                post.raw << "\n\n" << html
                post.save!
                PostUpload.create!(post: post, upload: upload) unless PostUpload.where(post: post, upload: upload).exists?
              end
            end
          end
        end
      end
    end
  end

  def find_attachment(filename, id)
    @attachments ||= Dir[File.join(BBOSS_ATTACHMENTS_DIR, "vf-attachs", "**", "*.*")]
    @attachments.find { |p| p.end_with?("/#{id}.#{filename}") }
  end

  def create_permalinks
    puts "", "creating permalinks..."

    last_topic_id = -1

    batches(BATCH_SIZE) do |offset|
      topics = bbpress_query(<<-SQL
        SELECT id,
               guid
          FROM #{BBOSS_PREFIX}posts
         WHERE post_status <> 'spam'
           AND post_type IN ('topic')
           AND id > #{last_topic_id}
      ORDER BY id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if topics.empty?
      last_topic_id = topics[-1]["id"].to_i

      topics.each do |t|
        topic = topic_lookup_from_imported_post_id(t['id'])
        Permalink.create(url: URI.parse(t['guid']).path.chomp('/'), topic_id: topic[:topic_id]) rescue nil
      end
    end
  end

  def import_private_messages
    puts "", "importing private messages..."

    last_post_id = -1
    total_posts = bbpress_query("SELECT COUNT(*) count FROM #{BBOSS_PREFIX}bp_messages_messages").first["count"]

    threads = {}

    total_count = bbpress_query("SELECT COUNT(*) count FROM #{BBOSS_PREFIX}bp_messages_recipients").first["count"]
    current_count = 0

    batches(BATCH_SIZE) do |offset|
      rows = bbpress_query(<<-SQL
        SELECT thread_id, user_id
          FROM #{BBOSS_PREFIX}bp_messages_recipients
      ORDER BY id
         LIMIT #{BATCH_SIZE}
        OFFSET #{offset}
      SQL
      ).to_a

      break if rows.empty?

      rows.each do |row|
        current_count += 1
        print_status(current_count, total_count, get_start_time('private_messages'))

        threads[row['thread_id']] ||= {
          target_user_ids: [],
          imported_topic_id: nil
        }
        user_id = user_id_from_imported_user_id(row['user_id'])
        if user_id && !threads[row['thread_id']][:target_user_ids].include?(user_id)
          threads[row['thread_id']][:target_user_ids] << user_id
        end
      end
    end

    batches(BATCH_SIZE) do |offset|
      posts =  bbpress_query(<<-SQL
        SELECT id,
               thread_id,
               date_sent,
               sender_id,
               subject,
               message
          FROM wp_bp_messages_messages
         WHERE id > #{last_post_id}
      ORDER BY thread_id, date_sent
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if posts.empty?

      last_post_id = posts[-1]["id"].to_i
      create_posts(posts, total: total_posts, offset: offset) do |post|
        clean_message = post['message']
        # for some bizzaro reason, there are backslashed single quotes all over the PMs
        clean_message.gsub!(/\\'/,"'")
        title = post['subject']
        title.gsub!(/\\'/,"'")
        if tcf = TopicCustomField.where(name: 'bb_thread_id', value: post['thread_id']).first
          {
            id: "pm#{post['id']}",
            topic_id: threads[post['thread_id']][:imported_topic_id],
            user_id: user_id_from_imported_user_id(post['sender_id']) || find_user_by_import_id(post['sender_id'])&.id || -1,
            raw: clean_message,
            created_at: post['date_sent'],
          }
        else
          # First post of the thread
          participants = User.where(id: threads[post['thread_id']][:target_user_ids]).pluck(:username)
          {
            id: "pm#{post['id']}",
            archetype: Archetype.private_message,
            user_id: user_id_from_imported_user_id(post['sender_id']) || find_user_by_import_id(post['sender_id'])&.id || -1,
            title: post['subject'],
            raw: clean_message,
            created_at: post['date_sent'],
            target_usernames: participants,
            participant_count: participants.length,
            post_create_action: proc do |new_post|
              if topic = new_post.topic
                threads[post['thread_id']][:imported_topic_id] = topic.id
                TopicCustomField.create(topic_id: topic.id, name: 'bb_thread_id', value: post['thread_id'])
              else
                puts "Error in post_create_action! Can't find topic!"
              end
            end
          }
        end
      end
    end

    def import_post_topics()
      puts "", "creating article comment topics and comments..."

      notes_category_id = category_id_from_imported_category_id(NOTES_CATEGORY_ID)
      last_post_id = -1
      total_posts = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BBOSS_PREFIX}posts
       WHERE post_status = 'publish'
         AND post_type IN ('post')
      SQL
      ).first["count"]

      batches(BATCH_SIZE) do |offset|
        posts = bbpress_query(<<-SQL
        SELECT id,
               post_author,
               post_date,
               post_content,
               post_title,
               post_type,
               post_parent
          FROM #{BBOSS_PREFIX}posts
         WHERE post_status = 'publish'
           AND post_type IN ('post')
           AND id > #{last_post_id}
      ORDER BY id
         LIMIT #{BATCH_SIZE}
      SQL
                             ).to_a

        break if posts.empty?

        last_post_id = posts[-1]["id"].to_i
        post_ids = posts.map { |p| p["id"].to_i }

        next if all_records_exist?(:posts, post_ids)

        post_ids_sql = post_ids.join(",")

        create_posts(posts, total: total_posts, offset: offset) do |p|
          skip = false
          ex = @posts_excerpts[p["id"]]

          if !ex
            skip = treu
          else
            user_id = user_id_from_imported_user_id(p["post_author"]) ||
                      find_user_by_import_id(p["post_author"]).try(:id) ||
                      -1

            post = {
              id: p["id"],
              user_id: user_id,
              raw: ex["baked"],
              tags: ex['categories'],
              created_at: p["post_date"],
              post_create_action: proc do |new_post|
                topic_embed_sql=<<-SQL
                insert  into topic_embeds (topic_id, post_id, embed_url, created_at, updated_at)
                        values(:topic_id, :post_id, :embed_url, :created_at, :updated_at)
                SQL
                DB.exec(topic_embed_sql,
                        topic_id: new_post[:topic_id],
                        post_id: new_post[:id],
                        embed_url: ex["link"],
                        created_at: new_post[:created_at],
                        updated_at: new_post[:updated_at])
              end
            }

            if post[:raw].present?
              post[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
            end

            post[:category] = notes_category_id
            post[:title] = CGI.unescapeHTML(p["post_title"])
          end

          skip ? nil : post
        end
      end
    end
  end

  def import_comment_users
    puts "", "importing comment users..."

    last_user_id = -1
    total_users = bbpress_query(<<-SQL
      SELECT COUNT(DISTINCT(u.id)) AS cnt
      FROM #{BBOSS_PREFIX}users u
      LEFT JOIN #{BBOSS_PREFIX}comments p ON p.user_id = u.id
       WHERE comment_approved = 1
         AND comment_type IN ('comment')
         AND user_email LIKE '%@%'
    SQL
    ).first["cnt"]

    batches(BATCH_SIZE) do |offset|
      users = bbpress_query(<<-SQL
        SELECT u.id, user_nicename, display_name, user_email, user_registered, user_url, user_pass
          FROM #{BBOSS_PREFIX}users u
          LEFT JOIN #{BBOSS_PREFIX}comments p ON p.user_id = u.id
         WHERE user_email LIKE '%@%'
           AND comment_type IN ('comment')
           AND comment_approved = 1
           AND u.id > #{last_user_id}
      GROUP BY u.id
      ORDER BY u.id
         LIMIT #{BATCH_SIZE}
      SQL
      ).to_a

      break if users.empty?

      last_user_id = users[-1]["id"]
      user_ids = users.map { |u| u["id"].to_i }

      next if all_records_exist?(:users, user_ids)

      user_ids_sql = user_ids.join(",")

      users_description = {}
      bbpress_query(<<-SQL
        SELECT user_id, meta_value description
          FROM #{BBOSS_PREFIX}usermeta
         WHERE user_id IN (#{user_ids_sql})
           AND meta_key = 'description'
      SQL
      ).each { |um| users_description[um["user_id"]] = um["description"] }

      users_last_activity = {}
      bbpress_query(<<-SQL
        SELECT user_id, meta_value last_activity
          FROM #{BBOSS_PREFIX}usermeta
         WHERE user_id IN (#{user_ids_sql})
           AND meta_key = 'last_activity'
      SQL
      ).each { |um| users_last_activity[um["user_id"]] = um["last_activity"] }

      create_users(users, total: total_users, offset: offset) do |u|
        {
          id: u["id"].to_i,
          username: u["user_nicename"],
          password: u["user_pass"],
          email: u["user_email"].downcase,
          name: u["display_name"].presence || u['user_nicename'],
          created_at: u["user_registered"],
          website: u["user_url"],
          bio_raw: users_description[u["id"]],
          last_seen_at: users_last_activity[u["id"]],
        }
      end
    end
  end

  def import_post_comments()
    puts "", "creating article comment topics and comments..."
    # Assumes post_id is the topic_id
    # comment IDs will need a remap
    last_comment_id = -1
    total_comments = bbpress_query(<<-SQL
      SELECT COUNT(*) count
        FROM #{BBOSS_PREFIX}comments
       WHERE comment_approved = 1
         AND comment_type IN ('comment')
      SQL
                               ).first["count"]

    comment_id_map = {}
    topic_post_number_counters = {}

    batches(BATCH_SIZE) do |offset|
      comments = bbpress_query(<<-SQL
        SELECT c.comment_id id,
               c.comment_post_id as comment_post_id,
               c.user_id as user_id,
               c.comment_date as comment_date,
               c.comment_content as comment_content,
               c.comment_parent as comment_parent,
               cp.user_id parent_user_id
          FROM #{BBOSS_PREFIX}comments c
          LEFT OUTER JOIN #{BBOSS_PREFIX}comments cp on cp.comment_id = c.comment_parent
         WHERE c.comment_approved = 1
           AND c.comment_type IN ('comment')
           AND c.comment_id > #{last_comment_id}
      ORDER BY c.comment_id
         LIMIT #{BATCH_SIZE}
      SQL
                           ).to_a

      break if comments.empty?
      break if offset > 1001
      last_comment_id = comments[-1]["id"].to_i
      comment_ids = comments.map { |p| p["id"].to_i }

      next if all_records_exist?(:posts, comment_ids)

      likes = bbpress_query(<<-SQL
                            SELECT comment_id, user_id from #{BBOSS_PREFIX}ulike_comments
                            WHERE comment_id in (#{comment_ids.join(',')})
                            ORDER BY comment_id asc
                            SQL
                           ).to_a

      create_posts(comments, total: total_comments, offset: offset) do |p|
        skip = false
        post_id_val = "ct#{p['id']}"
        user_id = user_id_from_imported_user_id(p["user_id"]) ||
                  find_user_by_import_id(p["user_id"]).try(:id) ||
                  -1
        liker_ids = likes.select{ |l| l['comment_id'] == p['id'] }
                      .map{ |l| user_id_from_imported_user_id(p["user_id"]) ||
                            find_user_by_import_id(p["user_id"]).try(:id)  || -1 }.select{ |id| id != -1 }
        comment = {
          id: post_id_val,
          user_id: user_id,
          raw: p["comment_content"],
          created_at: p["comment_date"],
          post_create_action: proc do |new_post|
            create_post_like_actions(liker_ids, new_post[:topic_id], new_post[:id], user_id, new_post[:created_at])
            new_post[:like_count] = new_post[:like_score] = liker_ids.length
            new_post.save
          end
        }

        parent_imported_post_id = p["comment_parent"].to_i == 0 ? "#{p['comment_post_id']}" : "ct#{p['comment_parent']}"
        new_post_parent_id = post_id_from_imported_post_id(parent_imported_post_id)

        if parent = topic_lookup_from_imported_post_id(parent_imported_post_id)
          comment[:topic_id] = parent[:topic_id]
          if !topic_post_number_counters[parent[:topic_id]]
            topic_post_number_counters[parent[:topic_id]] = parent[:post_number]
          end
          topic_post_number = topic_post_number_counters[parent[:topic_id]] += 1
          comment[:post_number] = topic_post_number
          comment[:reply_to_post_number] = parent[:post_number] if parent[:post_number] > 1
          if p["parent_user_id"]
            parent_user_id = user_id_from_imported_user_id(p["parent_user_id"]) ||
                             find_user_by_import_id(p["parent_user_id"]).try(:id) ||
                             -1
            comment[:reply_to_user_id] = parent_user_id
          end

        else
          puts "Skipping #{p["id"]}", p
          skip = true
        end

        if comment[:raw].present?
          comment[:raw].gsub!(/\<pre\>\<code(=[a-z]*)?\>(.*?)\<\/code\>\<\/pre\>/im) { "```\n#{@he.decode($2)}\n```" }
        end
        skip ? nil : comment
      end

    end
  end


  def create_post_like_actions(liker_ids, topic_id, post_id, post_user_id, post_date)
    if liker_ids
      liker_ids.each do |liker_id|
        post_like = <<~SQL
                  INSERT INTO post_actions (user_id, post_action_type_id, post_id,
                              created_at, updated_at)
                      values (:liker_id, 2, :post_id, :post_date, :post_date)
              SQL
        DB.exec(post_like, liker_id: liker_id,
                post_id: post_id,
                post_date: post_date) rescue nil

        make_liked =<<~SQL
                  INSERT INTO user_actions (acting_user_id, user_id, action_type, target_topic_id,
                                           target_post_id, created_at, updated_at)
                      values (:liker_id, :post_user_id, 2, :topic_id, :post_id,
                               :post_date, :post_date)
              SQL
        DB.exec(make_liked, liker_id: liker_id,
                 post_id: post_id,
                 post_date: post_date,
                 post_user_id: post_user_id,
                 topic_id: topic_id) rescue nil

        make_like = <<~SQL
                  INSERT INTO user_actions (acting_user_id, user_id, action_type, target_topic_id,
                                           target_post_id, created_at, updated_at)
                      values (:liker_id, :liker_id, 1, :topic_id, :post_id,
                              :post_date, :post_date)
              SQL
        DB.exec(make_like, liker_id: liker_id,
                post_id: post_id,
                post_date: post_date,
                topic_id: topic_id) rescue nil
      end
    end
  end


  def bbpress_query(sql)
    @client.query(sql, cache_rows: false)
  end

end

ImportScripts::BuddyBoss.new.perform
