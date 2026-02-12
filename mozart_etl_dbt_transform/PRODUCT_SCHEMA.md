# Product Standard Output Schema

All tenant mart models must conform to these column structures.

## mart_item_master

| Column | Type | Description | Required |
|--------|------|-------------|----------|
| item_id | VARCHAR | 품목 ID | NOT NULL |
| item_name | VARCHAR | 품목명 | |
| item_type | VARCHAR | 품목 유형 | |
| item_group_id | VARCHAR | 품목 그룹 ID | |
| procurement_type | VARCHAR | 조달 유형 | |
| created_at | TIMESTAMP | 생성 일시 | |
| updated_at | TIMESTAMP | 수정 일시 | |
