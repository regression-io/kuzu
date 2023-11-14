#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace catalog {

class Property {
public:
    // TODO: these should be guarded as reserved property names.
    //    static constexpr std::string_view REL_FROM_PROPERTY_NAME = "_from_";
    //    static constexpr std::string_view REL_TO_PROPERTY_NAME = "_to_";
    //    static constexpr std::string_view REL_SRC_OFFSET_NAME = "_src_offset_";
    //    static constexpr std::string_view REL_DST_OFFSET_NAME = "_nbr_offset_";

    Property(std::string name, std::unique_ptr<common::LogicalType> dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_TABLE_ID} {}

    Property(std::string name, std::unique_ptr<common::LogicalType> dataType,
        common::property_id_t propertyID, common::table_id_t tableID)
        : name{std::move(name)}, dataType{std::move(dataType)},
          propertyID{propertyID}, tableID{tableID} {}

    inline std::string getName() const { return name; }

    inline common::LogicalType* getDataType() const { return dataType.get(); }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline void setPropertyID(common::property_id_t propertyID_) { this->propertyID = propertyID_; }

    inline void setTableID(common::table_id_t tableID_) { this->tableID = tableID_; }

    inline void rename(std::string newName) { this->name = std::move(newName); }

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<Property> deserialize(common::Deserializer& deserializer);

    inline std::unique_ptr<Property> copy() const {
        return std::make_unique<Property>(name, dataType->copy(), propertyID, tableID);
    }

    static std::vector<std::unique_ptr<Property>> copy(
        const std::vector<std::unique_ptr<Property>>& properties);

private:
    std::string name;
    std::unique_ptr<common::LogicalType> dataType;
    common::property_id_t propertyID;
    common::table_id_t tableID;
};

} // namespace catalog
} // namespace kuzu
