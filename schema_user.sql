
-- drop table user_to_user_profile_mapping;
-- create table user_to_user_profile_mapping (
-- 	user_id varchar not null
-- 	, user_profile_id int not null
-- 	, foreign key (user_id) references "User"(id)
-- 	, foreign key (user_profile_id) references user_profile(id)
-- );
-- create unique index user_to_user_profile_mapping_idx_user_id_user_profile_id on user_to_user_profile_mapping (user_id, user_profile_id);

drop table user_profile;
create table user_profile (
	id serial primary key
	, handle varchar not null
	, profile_name varchar not null
	, profile_picture varchar
	, email varchar not null
	, twitter varchar
	, linkedin varchar
	, bio_short varchar
	, bio varchar
-- labels
	, label_hiring bool
	, label_open_to_work bool
	, label_open_to_cofounder_matching bool
	, label_need_product_feedback bool
	, label_open_to_discover_new_project bool
	, label_fundraising bool
	, label_open_to_invest bool
	, label_on_core_team bool
	, label_text_hiring varchar
	, label_text_open_to_work varchar
	, label_text_open_to_discover_new_project varchar
-- 	building skills
	, skill_founder bool
	, skill_web3_domain_expert bool
	, skill_artist bool
	, skill_frontend_eng bool
	, skill_backend_eng bool
	, skill_fullstack_eng bool
	, skill_blockchain_eng bool
	, skill_data_eng bool
	, skill_data_science bool
	, skill_hareware_dev bool
	, skill_game_dev bool
	, skill_dev_ops bool
	, skill_product_manager bool
	, skill_product_designer bool
	, skill_token_designer bool
	, skill_technical_writer bool
	-- Growth Skills
	, skill_social_media_influencer bool
	, skill_i_bring_capital bool
	, skill_community_manager bool
	, skill_marketing_growth bool
	, skill_business_development bool
	, skill_developer_relations bool
	, skill_influencer_relations bool
	, skill_investor_relations bool
	, resume varchar
	, foreign key (email) references "User"(email)
);
CREATE UNIQUE INDEX user_profile_unique_idx_handle ON user_profile (handle);
CREATE UNIQUE INDEX user_profile_unique_idx_email ON user_profile (email);
-- join on email.
-- each email is a new identity.

select * from "User" limit 10;
select * from user_profile limit 10;
truncate table user_profile ;

-- insert into user_profile (
-- 	handle
-- 	, profile_name
-- 	, email
-- 	, profile_picture
-- )
-- values (
-- 	'darshan'
-- 	, 'darshan'
-- 	, 'darshanraju9@gmail.com'
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- )
-- ;

-- insert into user_profile
-- values (
-- 	1
-- 	, 'ethtomato'
-- 	, 'Junyu'
-- 	, 'prowessyang@gmail.com'
-- 	, 'ethtomato'
-- 	, 'yang-jun-yu'
-- 	, 'Co-founder @ innerCircle.ooo
-- Building the people connector for web3.
-- Finding other web3 builders is hard. Twitter is dominanted by a few influencer. Discord is so noisy and scammy. LinkedIn is filled with Web2 people. Time to build our own web3 presence, in a web3 way.
-- Join us at innerCircle.ooo'
-- 	, 'early users'
-- 	, 'data analytics'
-- 	, true
-- 	, false
-- 	, true
-- 	, false
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- 	, null
-- )
-- ;


-- insert into user_profile
-- values (
-- 	2
-- 	, '22222'
-- 	, 'Junyu22'
-- 	, 'prowessyang2@gmail.com'
-- 	, 'ethtomato2'
-- 	, 'yang-jun-yu'
-- 	, 'Co-founder2222'
-- 	, 'early users'
-- 	, 'data analytics'
-- 	, true
-- 	, false
-- 	, true
-- 	, false
-- 	, 'https://en.gravatar.com/userimage/67165895/bd41f3f601291d2f313b1d8eec9f8a4d.jpg?size=200'
-- 	, null
-- )
-- ;

select * from "User" where id = 'cl3yotf7m0006m04ls517vfrl';

select * from user limit 100;

select * from "Session" limit 10;
select *
from "Account"
where "providerAccountId" = '111140146197096860395'
limit 10;

select * from "User" limit 10;
select * from user_ limit 10;

COPY user_profile(
	profile_name,
	handle,
	email,
	bio_short,
	label_on_core_team,
	label_open_to_work,
	label_open_to_invest,
	label_open_to_discover_new_project,
	label_hiring,
	label_fundraising,
	label_need_product_feedback,
	label_text_open_to_work,
	label_text_open_to_discover_new_project,
	skill_product_designer,
	skill_fullstack_eng,
	skill_backend_eng,
	skill_artist,
	skill_product_manager,
	skill_marketing_growth,
	skill_community_manager,
	skill_data_science,
	skill_business_development,
	skill_technical_writer,
	skill_social_media_influencer,
	skill_web3_domain_expert,
	skill_founder,
	skill_i_bring_capital,
	skill_hareware_dev,
	linkedin,
	twitter )
FROM '/home/junyuyang/csv/responses_upload.csv'
DELIMITER ','
CSV HEADER;


ALTER TABLE user_profile DISABLE TRIGGER ALL;
ALTER TABLE user_profile ENABLE TRIGGER ALL;
