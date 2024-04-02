/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2023/06/28.
//

#include <sstream>
#include "sql/parser/value.h"
#include "storage/field/field.h"
#include "common/log/log.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include <regex>

const char *ATTR_TYPE_NAME[] = {"undefined", "chars", "ints", "dates", "floats", "null","booleans",};

static float stringToNumber(const std::string& str) {
    try {
        // Use std::stod to convert the string to a double
        return std::stof(str);
    } catch (const std::invalid_argument& ia) {
        // If the conversion fails because the string does not contain a valid floating-point number
        // (or if no conversion could be performed)
    } catch (const std::out_of_range& oor) {
        // If the converted value would fall out of the range of representable values by a double
    }
    // Return 0.0 if parsing fails or if the string contains invalid characters
    return 0.0;
}

template <typename T>
std::string numberToString(T number) {
  T absNumber = std::abs(number); // Remove sign as per requirement
  return std::to_string(absNumber);
}

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= FLOATS) {
    return ATTR_TYPE_NAME[type];
  }
  return "unknown";
}
AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return UNDEFINED;
}
Value::Value(int val)
{
  set_int(val);
}

Value::Value(float val)
{
  set_float(val);
}

Value::Value(bool val)
{
  set_boolean(val);
}

Value::Value(){
  set_null();
}

Value::Value(const char *s, int len /*= 0*/)
{
  // check whether s is a date
  // if s is a date, initialize as a date value
  std::string str(s);
  if(check_date(str)){
    std::regex date_pattern(R"(^(\d{4})-(\d{1,2})-(\d{1,2})$)");
    std::smatch matches;
    std::regex_match(str, matches, date_pattern);

    int year = std::stoi(matches[1].str());
    int month = std::stoi(matches[2].str());
    int day = std::stoi(matches[3].str());

        // Convert the date to an integer format YYYYMMDD
    int date_as_int = year * 10000 + month * 100 + day;
    
     set_date(date_as_int);
  }
    
  else
  // else initialize as a string
   set_string(s, len);
}

static bool is_valid_date(int y, int m, int d) {
    static int mon[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap = (y % 400 == 0) || (y % 100 != 0 && y % 4 == 0);
    return y > 0
        && m > 0 && m <= 12
        && d > 0 && d <= (m == 2 && leap ? 29 : mon[m]);
}

// Function to check if a string is a valid date in YYYY-MM-DD format
bool Value::check_date(const std::string& date_str) {
    std::regex date_pattern(R"(^(\d{4})-(\d{1,2})-(\d{1,2})$)");
    std::smatch matches;

    if (std::regex_match(date_str, matches, date_pattern)) {
        int year = std::stoi(matches[1].str());
        int month = std::stoi(matches[2].str());
        int day = std::stoi(matches[3].str());

        return is_valid_date(year, month, day);
    }
    return false;
}

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case CHARS: {
      set_string(data, length);
    } break;
    case INTS: {
      num_value_.int_value_ = *(int *)data;
      length_ = length;
    } break;
    case FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_ = length;
    } break;
    case BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_ = length;
    }break;
    case DATES: {
      num_value_.date_value_ = *(int *)data;
      length_ = length;
    }
    case NULLTYPE: {
      num_value_.null_value_ = *(int *)data;
      length_ = length;
    }
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{ 
  attr_type_ = INTS;
  num_value_.int_value_ = val;
  length_ = sizeof(val);
}

void Value::set_float(float val)
{
  attr_type_ = FLOATS;
  num_value_.float_value_ = val;
  length_ = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_ = BOOLEANS;
  num_value_.bool_value_ = val;
  length_ = sizeof(val);
}
void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}

void Value::set_date(int val){
  attr_type_ = DATES;
  num_value_.date_value_ = val;
  length_ = sizeof(val);
}

void Value::set_null(){
  attr_type_ = NULLTYPE;
  num_value_.null_value_ = 0;
  is_null_ = true;
}

void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case INTS: {
      set_int(value.get_int());
    } break;
    case FLOATS: {
      set_float(value.get_float());
    } break;
    case CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case CHARS: {
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case INTS: {
      os << num_value_.int_value_;
    } break;
    case FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case CHARS: {
      os << str_value_;
    } break;
    case DATES: {
    std::string date_str = std::to_string(num_value_.date_value_);
    if (date_str.length() == 8) { // Ensure the date string is the expected length
    // Insert dashes to format the date as YYYY-MM-DD
    date_str.insert(4, "-");
    date_str.insert(7, "-");
    }
    os << date_str;
    }break;
    case NULLTYPE: {
      return "NULL";
    }break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

int Value::compare(const Value &other) const
{
  if(this->attr_type_ == NULLTYPE || other.attr_type_ == NULLTYPE){
    return false;
  }
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        return common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case FLOATS: {
        return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case CHARS: {
        return common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case DATES: {
        return common::compare_int((void *)&this->num_value_.date_value_, (void *)&other.num_value_.date_value_);
      } break;
      case BOOLEANS: {
        return common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      }
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = this->num_value_.int_value_;
    return common::compare_float((void *)&this_data, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = other.num_value_.int_value_;
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
  }
else if (this->attr_type_ == CHARS && other.attr_type_ == INTS) {
    // Convert CHARS (string) to a double for comparison with an INT
    float this_data_double = stringToNumber(this->str_value_);
    // Since we're comparing with an INT, consider casting to int if exact matching is required
    int this_data_int = static_cast<int>(this_data_double);
    // Direct comparison of this int and other int
    return common::compare_int((void *)&this_data_int, (void *)&other.num_value_.int_value_);
} else if (this->attr_type_ == CHARS && other.attr_type_ == FLOATS) {
    // Convert CHARS (string) to a double for comparison with a FLOAT
    float this_data_double = stringToNumber(this->str_value_);
    // Direct comparison of this double and other float
    // Note: Converting other's float to double for precise comparison
    return common::compare_float((void *)&this_data_double, (void *)&other.num_value_.float_value_);
}
  else if (this->attr_type_ == INTS && other.attr_type_ == CHARS) {
    // Other direction: Convert other's string to float and compare with this int
    float other_data = stringToNumber(other.str_value_);
    float this_data = static_cast<float>(this->num_value_.int_value_); // Convert int to float for direct comparison
    return common::compare_float((void *)&this_data, (void *)&other_data);
} else if (this->attr_type_ == FLOATS && other.attr_type_ == CHARS) {
    // Convert other's string to float for comparison with this float
    float other_data = stringToNumber(other.str_value_);
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
}

  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

int Value::get_int() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case INTS: {
      return num_value_.int_value_;
    }
    case FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case INTS: {
      return float(num_value_.int_value_);
    } break;
    case FLOATS: {
      return num_value_.float_value_;
    } break;
    case BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const
{
  return this->to_string();
}

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}
