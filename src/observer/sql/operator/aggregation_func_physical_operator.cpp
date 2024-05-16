#include "aggregation_func_physical_operator.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "sql/expr/tuple.h"
#include "sql/parser/value.h"

RC AggregationPhysicalOperator::open(Trx *trx)
{
  for (auto &child : children_) {
    auto rc = child->open(trx);
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC AggregationPhysicalOperator::next()
{
  if (already_run_) {
    return RC::RECORD_EOF;
  }
  RC   rc    = RC::SUCCESS;
  auto child = children_[0].get();
  result_.resize(fields_.size());
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }
    for (size_t i = 0; i < fields_.size(); i++) {
      auto &field = fields_[i];
      switch (field.get_aggr_type()) {
        case AggregationType::MAX: {
          Value value;
          tuple->find_cell({field.table_name(), field.meta()->name()}, value);
          if (result_[i].first) {
            result_[i].max_or_min = value;
            result_[i].first      = false;
          } else if (result_[i].max_or_min.compare(value) < 0) {
            result_[i].max_or_min = value;
          }
        } break;
        case AggregationType::MIN: {
          Value value;
          tuple->find_cell({field.table_name(), field.meta()->name()}, value);
          if (result_[i].first) {
            result_[i].max_or_min = value;
            result_[i].first      = false;
          } else if (result_[i].max_or_min.compare(value) > 0) {
            result_[i].max_or_min = value;
          }
        } break;
        case AggregationType::COUNT: {
          result_[i].tot_count_++;
        } break;
        case AggregationType::AVG: {
          // get count
          result_[i].tot_count_++;
          Value value;
          tuple->find_cell({field.table_name(), field.meta()->name()}, value);
          value.match_field_type(AttrType::FLOATS);
          result_[i].float_sum_ += value.get_float();
        } break;
        case AggregationType::SUM: {
          Value value;
          tuple->find_cell({field.table_name(), field.meta()->name()}, value);
          switch (value.attr_type()) {
            case INTS: {
              result_[i].int_sum_ += value.get_int();
              result_[i].is_float_sum_ = false;
            } break;
            case FLOATS: {
              result_[i].float_sum_ += value.get_float();
              result_[i].is_float_sum_ = true;
            } break;
            case CHARS: {
              value.match_field_type(AttrType::FLOATS);
              result_[i].float_sum_ += value.get_float();
              result_[i].is_float_sum_ = true;
            } break;
            case UNDEFINED:
            case DATES:
            case BOOLEANS: return RC::UNIMPLENMENT;
          }
          // todo
        } break;
        case AggregationType::NONE: {
          return RC::UNIMPLENMENT;
        } break;
      }
    }
  }

  // build a tuple
  auto tuple_ptr = new AggregationTuple;

  std::vector<Value> result;
  for (size_t i = 0; i < fields_.size(); i++) {
    tuple_ptr->add_cell_spec(fields_[i].field_name());
    switch (fields_[i].get_aggr_type()) {
      case AggregationType::MAX: {
        result.push_back(result_[i].max_or_min);
      } break;
      case AggregationType::MIN: {
        result.push_back(result_[i].max_or_min);
      } break;
      case AggregationType::COUNT: {
        Value res;
        res.set_int(result_[i].tot_count_);
        result.push_back(res);
      } break;
      case AggregationType::AVG: {
        Value res;
        res.set_float(result_[i].float_sum_ / result_[i].tot_count_);
        result.push_back(res);
      } break;
      case AggregationType::SUM: {
        Value res;
        if (result_[i].is_float_sum_)
          res.set_float(result_[i].float_sum_);
        else
          res.set_int(result_[i].int_sum_);
        result.push_back(res);
      } break;
      case AggregationType::NONE: {
        LOG_PANIC("WARN AGGR TYPE");
      } break;
    };
  }

  tuple_ptr->set_cells(result);
  current_tuple_     = tuple_ptr;
  this->already_run_ = true;
  return RC::SUCCESS;
}

RC AggregationPhysicalOperator::close()
{
  if (!children_.empty()) {
    auto rc = children_[0]->close();
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  delete current_tuple_;
  return RC::SUCCESS;
}