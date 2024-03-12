/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "common/lang/string.h"
#include <sstream>
InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db,  InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  Value *values = inserts.values.data();
  const int value_num = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values[i].attr_type();
    if (field_type != value_type) {  // TODO try to convert the value type to field type
      if (field_type != value_type) {
        // Attempt to convert the value type to match the field type
        if (field_type == INTS && value_type == CHARS) {
            // Convert string to int
            std::istringstream iss(values[i].get_string());
            int num;
            iss >> num;
            values[i].set_int(num);
        } else if (field_type == FLOATS && value_type == CHARS) {
            float num = 0.0f; // Default to 0
            try {
                num = std::stof(values[i].get_string());
            } catch (const std::invalid_argument& ia) {
                std::cerr << "Invalid argument: Unable to convert '" << values[i].get_string() << "' to float. " << ia.what() << std::endl;
            } catch (const std::out_of_range& oor) {
                std::cerr << "Out of range: '" << values[i].get_string() << "' is outside the range of representable values for a float. " << oor.what() << std::endl;
            }    // Update the Value object with the parsed float number
            values[i].set_float(num);
        } else if ((field_type == CHARS) && (value_type == INTS || value_type == FLOATS)) {
            // Convert int or float to string
            if (value_type == INTS) {
                    values[i].set_string(std::to_string(values[i].get_int()).c_str());
                } else if (value_type == FLOATS) {
                    std::ostringstream ss;
                    ss << values[i].get_float();
                    std::string s(ss.str());
                    values[i].set_string(s.c_str());
                }
        } else if(field_type == INTS && value_type == FLOATS){
               // Convert float to int using rounding (四舍五入)
                  float floatValue = values[i].get_float();
                  int roundedInt;
                  if (floatValue >= 0) {
                      roundedInt = static_cast<int>(floatValue + 0.5f);
                  } else {
                      roundedInt = static_cast<int>(floatValue - 0.5f);
                  }
                  values[i].set_int(roundedInt);
        }
          else if(field_type == FLOATS && value_type == INTS){
             // Convert int to float directly
            float num = static_cast<float>(values[i].get_int());
            values[i].set_float(num);    
          } 
        else {
            LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
                table_name, field_meta->name(), field_type, value_type);
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      }
    }
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num);
  return RC::SUCCESS;
}
