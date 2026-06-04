package nu.mine.mosher.genealogy;

import java.util.List;

public class FtmSchema {
    // Names of all tables with SyncVersion column
    /*
        SELECT m.name AS table_name
        FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p
        WHERE m.type = 'table' AND p.name = 'SyncVersion'
        ORDER BY table_name
     */
    public static final List<String> rSyncVersionTables = List.of(
        "ChildRelationship",
        "Deleted",
        "Deleted_Detail",
        "Fact",
        "FactType",
        "MasterSource",
        "MediaFile",
        "MediaLink",
        "Note",
        "Person",
        "PersonExternal",
        "Place",
        "Publication",
        "Relationship",
        "Repository",
        "Source",
        "SourceLink",
        "Tag",
        "TagLink",
        "Task",
        "Watermark",
        "WebLink");

    // (Child) tables that have LinkTableID and LinkID columns.
    // Each row *SHOULD* point to a parent row in another table.
    public static final List<String> rLinkChildTables = List.of(
        "Fact",
        "Note",
        "Task",
        "MediaLink",
        "SourceLink",
        "WebLink",
        "TagLink");

    // All tables that can be linked to, as long as they still exist.
    // Index within list is "LinkTableID" column value.
    // Empty strings indicate tables that don't exist (anymore).
    public static final List<String> rLinkParentTablesByNumber =
        List.of(
        /*  0 */ "",
        /*  1 */ "ChildRelationship",
        /*  2 */ "Fact",
        /*  3 */ "FactType",
        /*  4 */ "Note",
        /*  5 */ "Person",
        /*  6 */ "Place",
        /*  7 */ "Relationship",
        /*  8 */ "Setting",
        /*  9 */ "Task",
        /* 10 */ "MasterSource",
        /* 11 */ "",
        /* 12 */ "Repository",
        /* 13 */ "MediaFile",
        /* 14 */ "MediaLink",
        /* 15 */ "",
        /* 16 */ "Source",
        /* 17 */ "SourceLink",
        /* 18 */ "",
        /* 19 */ "",
        /* 20 */ "",
        /* 21 */ "Publication",
        /* 22 */ "HistoryList",
        /* 23 */ "Deleted",
        /* 24 */ "",
        /* 25 */ "WebLink",
        /* 26 */ "Tag",
        /* 27 */ "TagLink",
        /* 28 */ "",
        /* 29 */ "",
        /* 30 */ "PersonExternal",
        /* 31 */ "ChangeMacroCommand",
        /* 32 */ "ChangeCommand",
        /* 33 */ "DynamicFilter",
        /* 34 */ "DynamicFilterItem",
        /* 35 */ "Watermark",
        /* 36 */ "DnaMatch",
        /* 37 */ "",
        /* 38 */ "MediaFileOriginal");

    public static int linkTableIDof(final String nameParentTable) {
        final var i = rLinkParentTablesByNumber.indexOf(nameParentTable);
        if (nameParentTable.isBlank() || i < 0 || rLinkParentTablesByNumber.size() <= i) {
            throw new IllegalStateException();
        }
        return i;
    }
}
