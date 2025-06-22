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

 Date: 22/06/2025 21:50:00
*/


-- ----------------------------
-- Table structure for spider_status
-- ----------------------------
DROP TABLE IF EXISTS "public"."spider_status";
CREATE TABLE "public"."spider_status" (
  "name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "status" varchar COLLATE "pg_catalog"."default" DEFAULT 'enabled'::character varying,
  "last_update" timestamp(6)
)
;
ALTER TABLE "public"."spider_status" OWNER TO "postgres";

-- ----------------------------
-- Records of spider_status
-- ----------------------------
BEGIN;
-- News spiders (13 spiders)
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('tass', 'enabled', '2025-06-14 20:29:13.602915');
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('rbc', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('vedomosti', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('pnp', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('lenta', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('graininfo', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('forbes', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('interfax', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('izvestia', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('gazeta', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('rg', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('kommersant', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('ria', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('meduza', 'enabled', NULL);

-- Government and official spiders (4 spiders)
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('government', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('kremlin', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('regulation', 'enabled', NULL);

-- Legal document spiders (3 spiders)
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('pravo', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('sozd', 'enabled', NULL);
INSERT INTO "public"."spider_status" ("name", "status", "last_update") VALUES ('eaeu', 'enabled', NULL);
COMMIT;

-- ----------------------------
-- Primary Key structure for table spider_status
-- ----------------------------
ALTER TABLE "public"."spider_status" ADD CONSTRAINT "spider_status_pkey" PRIMARY KEY ("name");
