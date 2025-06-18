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

 Date: 14/06/2025 22:26:03
*/


-- ----------------------------
-- Table structure for spider_status
-- ----------------------------
DROP TABLE IF EXISTS "public"."spider_status";
CREATE TABLE "public"."spider_status" (
  "name" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "enabled" bool NOT NULL DEFAULT true,
  "status" varchar COLLATE "pg_catalog"."default" DEFAULT 'ok'::character varying,
  "last_update" timestamp(6)
)
;
ALTER TABLE "public"."spider_status" OWNER TO "postgres";

-- ----------------------------
-- Records of spider_status
-- ----------------------------
BEGIN;
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('tass', 't', 'ok', '2025-06-14 20:29:13.602915');
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('rbc', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('vedomosti', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('pnp', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('lenta', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('graininfo', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('forbes', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('interfax', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('izvestia', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('gazeta', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('rg', 't', 'ok', NULL);
INSERT INTO "public"."spider_status" ("name", "enabled", "status", "last_update") VALUES ('kommersant', 't', 'ok', NULL);
COMMIT;

-- ----------------------------
-- Primary Key structure for table spider_status
-- ----------------------------
ALTER TABLE "public"."spider_status" ADD CONSTRAINT "spider_status_pkey" PRIMARY KEY ("name");
