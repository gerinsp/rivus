package meta

// IcebergMaintenanceReservationSelector identifies a physical Iceberg target
// scope. NamespacePattern and TablePattern use path.Match syntax so one
// streaming reservation can protect both existing and future matching tables.
type IcebergMaintenanceReservationSelector struct {
	Catalog          string
	NamespacePattern string
	TablePattern     string
}
