#pragma once

#include "ddl.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

class DropProperty : public DDL {
public:
    DropProperty(Catalog* catalog, table_id_t tableID, property_id_t propertyID,
        StorageManager& storageManager, uint32_t id, const string& paramsString)
        : DDL{PhysicalOperatorType::DROP_PROPERTY, catalog, id, paramsString}, tableID{tableID},
          propertyID{propertyID}, storageManager{storageManager} {}

    string execute() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<DropProperty>(
            catalog, tableID, propertyID, storageManager, id, paramsString);
    }

protected:
    table_id_t tableID;
    property_id_t propertyID;
    StorageManager& storageManager;
};

} // namespace processor
} // namespace kuzu