/*
 Navicat Premium Dump SQL

 Source Server         : scrappy
 Source Server Type    : PostgreSQL
 Source Server Version : 140018 (140018)
 Source Host           : localhost:5432
 Source Catalog        : news_db
 Source Schema         : public

 Target Server Type    : PostgreSQL
 Target Server Version : 140018 (140018)
 File Encoding         : 65001

 Date: 14/06/2025 22:25:30
*/


-- ----------------------------
-- Table structure for articles
-- ----------------------------
DROP TABLE IF EXISTS "public"."articles";
CREATE TABLE "public"."articles" (
  "id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "text" text COLLATE "pg_catalog"."default" NOT NULL,
  "source" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "url" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "header" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "published_at" int4 NOT NULL,
  "published_at_iso" timestamp(6) NOT NULL,
  "parsed_at" int4 NOT NULL,
  "author" varchar COLLATE "pg_catalog"."default",
  "categories" json,
  "images" json,
  "created_at" timestamp(6),
  "updated_at" timestamp(6)
)
;
ALTER TABLE "public"."articles" OWNER TO "postgres";

-- ----------------------------
-- Records of articles
-- ----------------------------
BEGIN;
COMMIT;

-- ----------------------------
-- Uniques structure for table articles
-- ----------------------------
ALTER TABLE "public"."articles" ADD CONSTRAINT "articles_url_key" UNIQUE ("url");
ALTER TABLE "public"."articles" ADD CONSTRAINT "unique_url" UNIQUE ("url");

-- ----------------------------
-- Primary Key structure for table articles
-- ----------------------------
ALTER TABLE "public"."articles" ADD CONSTRAINT "articles_pkey" PRIMARY KEY ("id");
