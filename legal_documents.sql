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

 Date: 22/06/2025 21:45:00
*/


-- ----------------------------
-- Table structure for legal_documents
-- ----------------------------
DROP TABLE IF EXISTS "public"."legal_documents";
CREATE TABLE "public"."legal_documents" (
  "id" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "text" text COLLATE "pg_catalog"."default" NOT NULL,
  "original_id" varchar COLLATE "pg_catalog"."default",
  "doc_kind" varchar COLLATE "pg_catalog"."default",
  "title" text COLLATE "pg_catalog"."default",
  "source" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "url" varchar COLLATE "pg_catalog"."default" NOT NULL,
  "published_at" int4,
  "parsed_at" int4,
  "jurisdiction" varchar COLLATE "pg_catalog"."default",
  "language" varchar COLLATE "pg_catalog"."default",
  "stage" text COLLATE "pg_catalog"."default",
  "discussion_period" json,
  "explanatory_note" json,
  "summary_reports" json,
  "comment_stats" json,
  "created_at" timestamp(6) DEFAULT now(),
  "updated_at" timestamp(6) DEFAULT now()
)
;
ALTER TABLE "public"."legal_documents" OWNER TO "postgres";

-- ----------------------------
-- Records of legal_documents
-- ----------------------------
BEGIN;
COMMIT;

-- ----------------------------
-- Uniques structure for table legal_documents
-- ----------------------------
ALTER TABLE "public"."legal_documents" ADD CONSTRAINT "legal_documents_url_key" UNIQUE ("url");

-- ----------------------------
-- Primary Key structure for table legal_documents
-- ----------------------------
ALTER TABLE "public"."legal_documents" ADD CONSTRAINT "legal_documents_pkey" PRIMARY KEY ("id");

-- ----------------------------
-- Indexes structure for table legal_documents
-- ----------------------------
CREATE INDEX "idx_legal_documents_source" ON "public"."legal_documents" ("source");
CREATE INDEX "idx_legal_documents_jurisdiction" ON "public"."legal_documents" ("jurisdiction");
CREATE INDEX "idx_legal_documents_doc_kind" ON "public"."legal_documents" ("doc_kind");
CREATE INDEX "idx_legal_documents_published_at" ON "public"."legal_documents" ("published_at"); 