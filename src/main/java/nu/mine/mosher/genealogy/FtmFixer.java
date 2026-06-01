package nu.mine.mosher.genealogy;

import ch.qos.logback.classic.*;
import nu.mine.mosher.gnopt.Gnopt;
import org.slf4j.Logger;
import org.slf4j.*;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static java.util.Objects.requireNonNullElse;

/**
 * Updates any necessary Person identification value.
 * <p>
 * To determine the OPTIMAL identification value for a Person, choose from the
 * following Facts, by this order of preference:
 * </p>
 * <p>
 * 1. _ID (FactType.Abbreviation)
 * 2. UUID (FactType.Abbreviation)
 * 3. GUID (FactType.Abbreviation)
 * 4. ID (FactType.Abbreviation)
 * 5. REFN (FactType.Tag)
 * 6. Person.PersonGUID
 * </p>
 * <p>
 * For any of the Facts 1-5 in this list, there could be multiple for a given person,
 * with one and only one of them being marked as Preferred, and we will choose that one.
 * FTM enforces the one-and-only-one-preferred rule, we assume; if by chance there are
 * more than one such records, then we will choose an arbitrary one. If there are no
 * preferred records (per type), then we choose an arbitrary one (in order of preference).
 * </p>
 * <p>
 * If none exist (which should never happen, since FTM always provides a PersonGUID),
 * we generate a new random UUID.
 * </p>
 * <p>
 * This results in exactly one OPTIMAL identification UUID for the given Person.
 * </p>
 * <p>
 * If there is exactly one _ID record marked as Preferred, and it already has the OPTIMAL
 * identification as its value, then no update is necessary, so do nothing.
 * </p>
 * <p>
 * Otherwise, update any existing _ID records to mark them as NOT Preferred; then
 * insert one new _ID record, marked Preferred, with the OPTIMAL identification value
 * (as a string in standard UUID format).
 * </p>
 * <p>
 * The end result is intended to be as follows:
 * 1. Leave any existing UUID, GUID, ID, REFN, or PersonGUID values completely untouched.
 * 2. Do not remove any existing _ID values.
 * 3. There will be exactly one _ID value marked as Preferred, and
 * 4. it will have as its value the preferred UUID (OPTIMAL identification for the Person)
 * </p>
 * <p>
 * For any program that is trying to locate a person based on a UUID, it should check
 * all values (as in the SQL query in findOptimalUuid) for any matching UUID. This will allow
 * for permalinks, including handling merging of persons (where the resultant merged person
 * would contain both UUIDs).
 * </p>
 */
public class FtmFixer {
    private static final Logger LOG = LoggerFactory.getLogger(FtmFixer.class);
    private static FtmFixerOptions options;

    private final Connection db;
    private final Map<UUID, Long> uuids = new HashMap<>(8192);

    private FtmFixer(final Connection db) {
        this.db = db;
    }

    public static void main(final String... args) throws Gnopt.InvalidOption {
        options = Gnopt.process(FtmFixerOptions.class, args);

        final LoggerContext ctx = (LoggerContext)LoggerFactory.getILoggerFactory();
        final ch.qos.logback.classic.Logger LOG_ROOT = ctx.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        if (options.verbose) {
            LOG_ROOT.setLevel(Level.TRACE);
        } else {
            LOG_ROOT.setLevel(Level.INFO);
        }

        if (options.shouldRun) {
            if (options.files.isEmpty()) {
                LOG.error("Missing required argument: <tree>.ftm [...]");
                System.exit(1);
            }
            for (final String file : options.files) {
                fixDatabase(file);
            }
        } else {
            if (!options.files.isEmpty()) {
                LOG.warn("Ignored arguments: {}", options.files);
            }
        }

        LOG.debug("Program completed normally.");
    }

    private static void fixDatabase(final String arg) {
        try {
            final var path = Paths.get(arg);
            LOG.info("{}", new String(new char[70]).replace("\0", "*"));
            LOG.info("opening FTM tree file: {}", path);
            final var ftmFixer = new FtmFixer(DriverManager.getConnection("jdbc:sqlite:" + path));

            ftmFixer.verifySyncVersions();

            ftmFixer.fixOptimalUuid();
            ftmFixer.verifyXml();
            ftmFixer.findAnomalousRelationships();
            // TODO show Sources not associated with any facts...
        } catch (final Exception e) {
            LOG.error("Error processing {}", arg, e);
        }
    }

    private void verifySyncVersions() throws SQLException {
        final var syncVersion = readSyncVersion();

        List.of(
            "ChildRelationship",
            "Deleted",
            "Fact",
            "FactType",
            "MasterSource",
            "MediaFile",
            "MediaLink",
            "Note",
            "Person",
            "Place",
            "Publication",
            "Relationship",
            "Repository",
            "Source",
            "SourceLink",
            "Tag",
            "TagLink",
            "Task",
            "WebLink",
            "PersonExternal",
            "Watermark"
        ).forEach(s -> verifySyncVersionTable(syncVersion, s));
    }

    // All tables that can be linked to, as long as they still exist.
    // Index within list is "LinkTableID" column value.
    // Empty strings indicate tables that don't exist (anymore).
    private static final List<String> rTablesByNumber =
        List.of("",
        "ChildRelationship",
        "Fact",
        "FactType",
        "Note",
        "Person",
        "Place",
        "Relationship",
        "Setting",
        "Task",
        "MasterSource",
        "",
        "Repository",
        "MediaFile",
        "MediaLink",
        "",
        "Source",
        "SourceLink",
        "",
        "",
        "",
        "Publication",
        "HistoryList",
        "Deleted",
        "",
        "WebLink",
        "Tag",
        "TagLink",
        "",
        "",
        "PersonExternal",
        "ChangeMacroCommand",
        "ChangeCommand",
        "DynamicFilter",
        "DynamicFilterItem",
        "Watermark",
        "DnaMatch",
        "",
        "MediaFileOriginal");

    private void verifySyncVersionTable(int syncVersion, String table) {
        final var sql = "SELECT COUNT(*) FROM "+table+" WHERE SyncVersion > ?";
        try (final var q = this.db.prepareStatement(sql)) {
            q.setInt(1, syncVersion);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var c = rs.getInt(1);
                    if (c > 0) {
                        LOG.warn("Invalid SyncVersion rows found: {}: {}", table, c);
                        if (options.force) {
                            fixSyncVersionInTable(syncVersion, table);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void fixSyncVersionInTable(int syncVersion, String table) throws SQLException {
        final var sql = "UPDATE "+table+" SET SyncVersion = ? WHERE SyncVersion > ?";
        try (final var update = this.db.prepareStatement(sql)) {
            update.setInt(1, syncVersion);
            update.setInt(2, syncVersion);
            final var cUpdate = update.executeUpdate();
            LOG.info("    Updated row count: {}", cUpdate);
        }
    }

    /*
     * Analyze families ("Relationship" and "ChildRelationship" tables").
     *
     * Relationship holds two parents (PersonID1, PersonID2)
     * ChildRelationship links a child (PersonID) to the parents (RelationshipID)
     *
     * Nominal family cases:
     *      two parents (with or without children)
     *      one parent with at least one child.
     *
     * Strange cases (to report on):
     *      no parents and no children: "NoPeople"
     *      no parents and at least one child: "OnlyChildren"
     *      one parent with no children: "OnlyOneParent"
     *      people that are "child in" more than one family "MultipleFamilies"
     *
     * For each strange case, log it, along with related rows from Fact and Note tables.
     *
     */


    private void findAnomalousRelationships() throws SQLException {
        // "Rel" means "Relationship"
        final var relsNoPeople = selectIds("""
            SELECT id FROM Relationship
            WHERE
                ID NOT IN (
                    SELECT RelationshipID FROM ChildRelationship GROUP BY RelationshipID
                ) AND (
                    Person1ID IS NULL AND Person2ID IS NULL
                )
            """);
        final var relsOnlyChildren = selectIds("""
            SELECT id FROM Relationship
            WHERE
                ID IN (
                    SELECT RelationshipID FROM ChildRelationship GROUP BY RelationshipID
                ) AND (
                    Person1ID IS NULL AND Person2ID IS NULL
                )
            """);
        final var relsOnlyOneParent = selectIds("""
            SELECT id FROM Relationship
            WHERE
                ID NOT IN (
                    SELECT RelationshipID FROM ChildRelationship GROUP BY RelationshipID
                ) AND (
                    (Person1ID IS NOT NULL AND Person2ID IS NULL) OR
                    (Person2ID IS NOT NULL AND Person1ID IS NULL)
                )
            """);
        final var persMultipleFamilies = selectIds("""
            SELECT personid AS id FROM ChildRelationship GROUP BY personid HAVING COUNT(personid) > 1
            """);

        logRels(relsNoPeople, "Relationship wih no parents and no children");
        logRels(relsOnlyChildren, "Relationship wih no parents and at least one child");
        logRels(relsOnlyOneParent, "Relationship wih exactly one parent with no children");
        logChildOfMultipleFamilies(persMultipleFamilies, "Child in multiple families");
        // TODO implement automatic deleting? (is it OK with syncing?)
    }

    private void logChildOfMultipleFamilies(final SortedSet<Long> pers, final String msg) throws SQLException {
        for (final var p : pers) {
            LOG.warn("{}: ID={}", msg, p);
            logPerson(p, "child");
            logParentFamilies(p);
        }
    }

    private void logParentFamilies(final Long idPerson) throws SQLException {
        final var sql = """
            SELECT
                r.id AS relationshipid,
                p1.fullname AS parent1,
                p2.fullname AS parent2
            FROM
                childrelationship AS c LEFT OUTER JOIN
                relationship AS r ON (r.id = c.relationshipid) LEFT OUTER JOIN
                person AS p1 ON (p1.id = r.person1id) LEFT OUTER JOIN
                person AS p2 ON (p2.id = r.person2id)
            WHERE c.personid = ?
            """;
        final var rRelId = new ArrayList<Long>();
        final var rRelName = new ArrayList<String>();
        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idPerson);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var idRel = rs.getLong("relationshipid");
                    final var parent1 = getStringFrom("parent1", rs);
                    final var parent2 = getStringFrom("parent2", rs);
                    final String parents;
                    if (parent1.isBlank() && parent2.isBlank()) {
                        parents = "(no parents)";
                    } else if (parent2.isBlank()) {
                        parents = "one parent : "+parent1;
                    } else if (parent1.isBlank()) {
                        parents = "one parent : "+parent2;
                    } else {
                        parents = "two parents: "+parent1+" & "+parent2;
                    }
                    rRelId.add(idRel);
                    rRelName.add(parents);
                }
            }
        }
        for (int i = 0; i < rRelId.size(); ++i) {
            final var idRel = rRelId.get(i);
            final var sRel = rRelName.get(i);
            LOG.info("    relationship: ID={}, {}", idRel, sRel);
            logAllLinkedChildrenOf("        ", 7, idRel);
            LOG.info("    ----");
        }
    }

    private void logRels(final SortedSet<Long> rels, final String msg) throws SQLException {
        for (final var id : rels) {
            LOG.warn("{}: ID={}", msg, id);

            SortedSet<Long> people;
            people = selectIds("SELECT person1id AS id FROM relationship WHERE id = ?", Optional.of(id));
            for (final var p : people) {
                logPerson(p, "parent");
            }
            people = selectIds("SELECT person2id AS id FROM relationship WHERE id = ?", Optional.of(id));
            for (final var p : people) {
                logPerson(p, "parent");
            }
            people = selectIds("SELECT personid AS id FROM childrelationship WHERE relationshipid = ?", Optional.of(id));
            for (final var p : people) {
                logPerson(p, "child");
            }
//            logRelFacts(id);
            logAllLinkedChildrenOf("    ", 7, id);
            LOG.info("----");
        }
    }

//    private void logRelFacts(final Long idRelationship) throws SQLException {
//        final var idFacts = new ArrayList<Long>();
//        final var sql = """
//            SELECT
//                f.id AS factid,
//                f.date AS date,
//                f.text AS s,
//                '('||t.tag||') '||t.name AS type,
//                p.name AS place
//            FROM
//                fact AS f LEFT OUTER JOIN
//                facttype AS t ON (t.id = f.facttypeid) LEFT OUTER JOIN
//                place AS p ON (p.id = f.placeid)
//            WHERE f.linktableid = 7 AND f.linkid = ?
//            """;
//        try (final var q = this.db.prepareStatement(sql)) {
//            q.setLong(1, idRelationship);
//            try (final var rs = q.executeQuery()) {
//                while (rs.next()) {
//                    final var idFact = getLongFrom("factid", rs);
//                    final var date = getDateFrom("date", rs);
//                    final var s = getStringFrom("s", rs);
//                    final var type = getStringFrom("type", rs);
//                    final var place = getPlaceFrom("place", rs);
//                    LOG.info("    id=\"{}\", type=\"{}\", date=\"{}\", text=\"{}\", place=\"{}\"", idFact, type, date, s, place);
//                    idFacts.add(idFact);
//                }
//            }
//        }
//        for (final var idFact : idFacts) {
//            logFactSourceLinks(idFact);
//        }
//    }

    private void logSourceLinkDetails(final String indent, final Long idSourceLink) throws SQLException {
        final var sql = """
            SELECT
                sl.id AS sourcelinkid,
                sl.sourceid,
                s.pid,
                m.title,
                m.author
            FROM
                sourcelink AS sl LEFT OUTER JOIN
                source AS s ON (s.id = sl.sourceid) LEFT OUTER JOIN
                mastersource AS m ON (m.id = s.mastersourceid)
            WHERE
                sl.id = ?
            """;
        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idSourceLink);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var idSourceLinkRead = getLongFrom("sourcelinkid", rs);
                    final var idSource = getLongFrom("sourceid", rs);
                    final var sPid = getStringFrom("pid", rs);
                    final var sTitle = getStringFrom("title", rs);
                    final var sAuthor = getPlaceFrom("author", rs);
                    LOG.info("{}table=SourceLink, id={}, sourceid={} PID=\"{}\", title=\"{}\", author=\"{}\"",
                            indent, idSourceLinkRead, idSource, sPid, sTitle, sAuthor);
                }
            }
        }
    }

    private static final List<String> linkables = List.of("WebLink", "Task", "TagLink", "SourceLink", "Note", "MediaLink", "Fact");

    private void logAllLinkedChildrenOf(final String indent, final long tableParent, final long idParent) throws SQLException {
        /*
SELECT 'WebLink'   , id FROM WebLink    t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'Task'      , id FROM Task       t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'TagLink'   , id FROM TagLink    t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'SourceLink', id FROM SourceLink t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'Note'      , id FROM Note       t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'MediaLink' , id FROM MediaLink  t WHERE t.linktableid = 2 AND t.linkid = 1047 UNION ALL
SELECT 'Fact'      , id FROM Fact       t WHERE t.linktableid = 2 AND t.linkid = 1047
         */
        final var sb = new StringBuilder(1024);
        boolean first = true;
        for (final var linkable : linkables) {
            if (!first) {
                sb.append(" UNION ALL ");
            }
            sb.append("SELECT ");
            sb.append("\'").append(linkable).append("\' AS nameTable, ");
            sb.append("id FROM ").append(linkable).append(" AS t WHERE t.linktableid = ? AND t.linkid = ?");
            first = false;
        }
        final var sql = sb.toString();



        final var idFacts = new ArrayList<Long>();
        final var idSourceLinks = new ArrayList<Long>();
        final var idWebLinks = new ArrayList<Long>();
        final var idTasks = new ArrayList<Long>();
        final var idTagLinks = new ArrayList<Long>();
        final var idNotes = new ArrayList<Long>();
        final var idMediaLinks = new ArrayList<Long>();
        try (final var q = this.db.prepareStatement(sql)) {
            for (int i = 0; i < linkables.size(); ++i) {
                q.setLong(2*i+1, tableParent);
                q.setLong(2*i+2, idParent);
            }
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var sTableChild = getStringFrom("nameTable", rs);
                    final var idChild = getLongFrom("id", rs);
                    if (sTableChild.equals("Fact")) {
                        idFacts.add(idChild);
                    } else if (sTableChild.equals("SourceLink")) {
                        idSourceLinks.add(idChild);
                    } else if (sTableChild.equals("WebLink")) {
                        idWebLinks.add(idChild);
                    } else if (sTableChild.equals("Task")) {
                        idTasks.add(idChild);
                    } else if (sTableChild.equals("TagLink")) {
                        idTagLinks.add(idChild);
                    } else if (sTableChild.equals("Note")) {
                        idNotes.add(idChild);
                    } else if (sTableChild.equals("MediaLink")) {
                        idMediaLinks.add(idChild);
                    } else {
                        // can't happen
                    }
                }
            }
        }
        for (final var id : idFacts) {
            logFactDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 2, id);
        }
        for (final var id : idSourceLinks) {
            logSourceLinkDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 17, id);
        }
        for (final var id : idWebLinks) {
            logWebLinkDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 25, id);
        }
        for (final var id : idTasks) {
            logTaskDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 9, id);
        }
        for (final var id : idTagLinks) {
            logTagLinkDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 27, id);
        }
        for (final var id : idNotes) {
            logNoteDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 4, id);
        }
        for (final var id : idMediaLinks) {
            logMediaLinkDetails(indent, id);
            logAllLinkedChildrenOf(indent+"    ", 14, id);
        }
    }

    private void logWebLinkDetails(final String indent, final Long id) throws SQLException {
        final var nameTable = "WebLink";
        LOG.info("{}table={}, id={}", indent, nameTable, id);
    }

    private void logTaskDetails(final String indent, final Long id) throws SQLException {
        final var nameTable = "Task";
        LOG.info("{}table={}, id={}", indent, nameTable, id);
    }

    private void logTagLinkDetails(final String indent, final Long id) throws SQLException {
        final var nameTable = "TagLink";
        LOG.info("{}table={}, id={}", indent, nameTable, id);
    }

    private void logNoteDetails(final String indent, final Long id) throws SQLException {
        final var sql = "SELECT notetext AS s FROM note WHERE id = ?";
        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, id);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var s = getStringFrom("s", rs);
                    final var nameTable = "Note";
                    LOG.info("{}table={}, id={} note=\"{}\"",
                        indent, nameTable, id, s);
                }
            }
        }
    }

    private void logMediaLinkDetails(final String indent, final Long id) throws SQLException {
        final var nameTable = "MediaLink";
        LOG.info("{}table={}, id={}", indent, nameTable, id);
    }

    private void logFactDetails(final String indent, final Long idFact) throws SQLException {
        final var sql = """
            SELECT
                f.id AS factid,
                '('||t.tag||') '||t.name AS type,
                f.text AS s,
                f.date AS date,
                p.name AS place
            FROM
                fact AS f LEFT OUTER JOIN
                facttype AS t ON (t.id = f.facttypeid) LEFT OUTER JOIN
                place AS p ON (p.id = f.placeid)
            WHERE
                f.id = ?
            """;
        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idFact);
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var idFactRead = getLongFrom("factid", rs);
                    final var date = getDateFrom("date", rs);
                    final var s = getStringFrom("s", rs);
                    final var type = getStringFrom("type", rs);
                    final var place = getPlaceFrom("place", rs);
                    LOG.info("{}table=Fact, id={}, type=\"{}\", text=\"{}\", date=\"{}\", place=\"{}\"",
                        indent, idFactRead, type, s, date, place);
                }
            }
        }
    }




    private Long getLongFrom(final String col, final ResultSet rs) throws SQLException {
        final var s = getStringFrom(col, rs);
        return Long.parseLong(s, 10);
    }

    private String getPlaceFrom(final String col, final ResultSet rs) throws SQLException {
        final var factplace = getStringFrom(col, rs);
        if (factplace.isBlank()) {
            return "";
        }
        return FtmPlace.fromFtmPlace(factplace).toString();
    }

    private String getDateFrom(final String col, ResultSet rs) throws SQLException {
        final var factdate = getStringFrom(col, rs);
        if (factdate.isBlank()) {
            return "";
        }
        return FtmDate.fromFtmFactDate(factdate).toString();
    }

    private String getStringFrom(final String col, final ResultSet rs) throws SQLException {
        final var s = rs.getString(col);
        if (rs.wasNull()) {
            return "";
        }
        if (s.isBlank()) {
            return "";
        }
        return s.strip();
    }

    private void logPerson(final Long p, final String msg) throws SQLException {
        final var s = selectOneString("SELECT fullname AS s FROM person WHERE id = ?", Optional.of(p));
        LOG.info("    {}: ID={}, name=\"{}\"", msg, p, s);
    }

    // string column must be named as "s"
    private String selectOneString(final String sql, final Optional<Long> optID) throws SQLException {
        String ret = "";
        try (final var q = this.db.prepareStatement(sql)) {
            if (optID.isPresent()) {
                q.setLong(1, optID.get());
            }
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    ret = getStringFrom("s", rs);
                }
            }
        }
        return ret;
    }

    /**
     * Selects a set of "id" column values (long), from rows returned by the given SQL SELECT.
     * @param sql "SELECT col AS id FROM tab WHERE id = ?"
     * @return sorted (possibly empty) set of IDs (never returns null, and no nulls will be in the set)
     * @throws SQLException
     */
    private SortedSet<Long> selectIds(final String sql, final Optional<Long> optID) throws SQLException {
        final var ret = new TreeSet<Long>();
        try (final var q = this.db.prepareStatement(sql)) {
            if (optID.isPresent()) {
                q.setLong(1, optID.get());
            }
            try (final var rs = q.executeQuery()) {
                while (rs.next()) {
                    final var id = rs.getLong("id");
                    if (!rs.wasNull()) {
                        ret.add(id);
                    }
                }
            }
        }
        return Collections.unmodifiableSortedSet(ret);
    }

    /**
     * Selects a set of "id" column values (long), from rows returned by the given SQL SELECT.
     * @param sql "SELECT col AS id FROM tab"
     * @return sorted (possibly empty) set of IDs (never returns null, and no nulls will be in the set)
     * @throws SQLException
     */
    private SortedSet<Long> selectIds(final String sql) throws SQLException {
        return selectIds(sql, Optional.empty());
    }



    private void verifyXml() throws SQLException {
        final var sql = """
            SELECT
                Source.PageNumber, MasterSource.Title, Person.FullName
            FROM
                Fact INNER JOIN
                SourceLink ON (SourceLink.LinkID = Fact.ID AND SourceLink.LinkTableID = 2) INNER JOIN
                Source ON (Source.ID = SourceLink.SourceID) INNER JOIN
                MasterSource ON (MasterSource.ID = Source.MasterSourceID) INNER JOIN
                Person ON (Person.ID = Fact.LinkID AND Fact.LinkTableID = 5)
            WHERE
                PageNumber IS NOT NULL AND
                SUBSTR(PageNumber,1,1) = '<'
            GROUP BY
                Source.ID
            """;
        try (final var q = this.db.prepareStatement(sql); final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var citation = rs.getString("PageNumber");
                final var title = requireNonNullElse(rs.getString("Title"), "");
                final var person = requireNonNullElse(rs.getString("FullName"), "");
                if (!validCitation(citation)) {
                    LOG.warn("    INVALID XML found for citation starting with '<':");
                    LOG.warn("    TITLE: {}", title);
                    LOG.warn("    PERSON: {}", person);
                }
            }
        }
    }

    private boolean validCitation(final String citation) {
        boolean valid = false;
        try {
            final var factory = DocumentBuilderFactory.newInstance();
            final var builder = factory.newDocumentBuilder();
            final var dom = builder.parse(new InputSource(new BufferedReader(new StringReader(citation))));
            final var rBibl = dom.getElementsByTagName("bibl");
            for (int i = 0; i < rBibl.getLength(); ++i) {
                final var bibl = rBibl.item(i);
                final var firstChar = bibl.getTextContent().substring(0,1);
                if (firstChar.isBlank()) {
                    LOG.warn("bibl element starts with whitespace");
                    return false;
                }
            }
            valid = true;
        } catch (final Exception ignore) {
        }
        return valid;
    }

    private void fixOptimalUuid() throws SQLException {
        final var syncVersion = readSyncVersion();

        if (findFactType_ID() == 0L && options.force) {
            createFactType(syncVersion);
            if (findFactType_ID() == 0L) {
                LOG.error("Could not create FactType _ID record.");
            }
        }

        final var idFactType = findFactType_ID();

        final var sql = "SELECT ID, FullName FROM Person";
        try (final var q = this.db.prepareStatement(sql); final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var idPerson = rs.getLong("ID");
                final var name = requireNonNullElse(rs.getString("FullName"), "");
                LOG.debug("Person: ID={}, FullName={}", idPerson, name);

                final var optimal = findOptimalUuid(idPerson);

                // if C != 1 then DO_UPDATE, else
                // if Text exactly equals OPTIMAL, then do nothing, else DO_UPDATE
                if (!isExactlyOnePreferred_ID(idPerson, name) || !isMatching_ID(idPerson, optimal)) {
                    doUpdate(idPerson, idFactType, optimal, syncVersion);
                    if (options.force) {
                        findOptimalUuid(idPerson); // re-log IDs
                    }
                }
            }
        }
    }

    private UUID findOptimalUuid(final long idPerson) throws SQLException {
        final var sql = """
            SELECT * FROM (
            SELECT
                Fact.ID,
                FactType.Tag,
                FactType.Abbreviation,
                Fact.Preferred,
                LOWER(Fact.Text) AS Text,
                Fact.SyncVersion
            FROM
                Fact JOIN
                FactType ON (FactType.ID = Fact.FactTypeID)
            WHERE
                Fact.LinkID = ? AND
                Fact.LinkTableID = 5 AND
                (FactType.Abbreviation IN ('_ID','ID','GID','UID') OR FactType.Tag = 'REFN')
            UNION
            SELECT
                NULL AS ID,
                '~~~~' AS Tag,
                NULL AS Abbreviation,
                1 AS Preferred,
                LOWER(
                    SUBSTR(HEX(PersonGUID), 7,2)||
                    SUBSTR(HEX(PersonGUID), 5,2)||
                    SUBSTR(HEX(PersonGUID), 3,2)||
                    SUBSTR(HEX(PersonGUID), 1,2)||'-'||
                    SUBSTR(HEX(PersonGUID),11,2)||
                    SUBSTR(HEX(PersonGUID), 9,2)||'-'||
                    SUBSTR(HEX(PersonGUID),15,2)||
                    SUBSTR(HEX(PersonGUID),13,2)||'-'||
                    SUBSTR(HEX(PersonGUID),17,4)||'-'||
                    SUBSTR(HEX(PersonGUID),21)) AS Text,
                SyncVersion
            FROM
                Person
            WHERE
                ID = ?
            )
            ORDER BY
                Tag,
                CASE Abbreviation
                    WHEN '_ID' THEN 0
                    WHEN  'ID' THEN 1
                    WHEN 'UID' THEN 2
                    WHEN 'GID' THEN 3
                               ELSE 4
                END,
                Preferred DESC
            """;
        // go through and choose the first value that's in valid UUID format,
        // and that will be the OPTIMAL identification value

        Optional<UUID> optimal = Optional.empty();

        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idPerson);
            q.setLong(2, idPerson);
            try (final var rs = q.executeQuery()) {
                final var header = String.format("    %7s %4s %4s %1s %-40s %1s %5s", "Fact ID", "Tag", "Abrv", "P", "Text", "U", "SyncVersion");
                LOG.debug(header);
                while (rs.next()) {
                    final var idFact = rs.getLong("ID");
                    final var tag = requireNonNullElse(rs.getString("Tag"), "");
                    final var abbreviation = requireNonNullElse(rs.getString("Abbreviation"), "");
                    final var preferred = rs.getInt("Preferred") != 0;
                    final var text = requireNonNullElse(rs.getString("Text"), "");
                    final var syncVersion = rs.getInt("SyncVersion");
                    final var optUuid = asUuid(text);

                    final var msg = String.format("    %7d %4s %4s %c %-40s %c %5d", idFact, tag, abbreviation, preferred ? '*' : ' ', text, optUuid.isPresent() ? '*' : ' ', syncVersion);
                    LOG.debug(msg);

                    if (optUuid.isPresent()) {
                        checkDuplicateUuid(optUuid.get(), idPerson);

                        if (optimal.isEmpty()) {
                            optimal = optUuid;
                        }
                    }
                }
            }
        }

        if (optimal.isEmpty()) {
            final var uuid = UUID.randomUUID();
            checkDuplicateUuid(uuid, idPerson);
            optimal = Optional.of(uuid);
            LOG.warn("    Could not find any valid UUID, so one was generated: {}", optimal.get());
        }

        LOG.debug("    OPTIMAL UUID: ----> {} <----------------", optimal.get());

        return optimal.get();
    }

    private void checkDuplicateUuid(final UUID uuid, final long idPerson) {
        if (this.uuids.containsKey(uuid)) {
            final long idPersonExisting = this.uuids.get(uuid);
            if (idPersonExisting != idPerson) {
                LOG.error("DUPLICATE UUID DETECTED: {} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<", uuid);
            }
        } else {
            this.uuids.put(uuid, idPerson);
        }
    }

    private boolean isExactlyOnePreferred_ID(final long idPerson, final String name) throws SQLException {
        final var sql = """
            SELECT
                COUNT(*) AS C
            FROM
                Fact
            WHERE
                LinkID = ? AND
                LinkTableID = 5 AND
                FactTypeID = (SELECT FactType.ID FROM FactType WHERE FactType.Abbreviation = '_ID') AND
                Preferred = 1
            """;

        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idPerson);
            try (final var rs = q.executeQuery()) {
                if (rs.next()) {
                    final var c = rs.getInt("C");
                    if (c == 1) {
                        LOG.debug("    count of existing Preferred _ID Facts: {}", c);
                    }
                    else {
                        LOG.info("Person: ID={}, FullName={}", idPerson, name);
                        LOG.info("    count of existing Preferred _ID Facts: {} <=======================", c);
                    }
                    return c == 1;
                }
            }
        }

        return false;
    }

    private boolean isMatching_ID(final long idPerson, final UUID uuidOptimal) throws SQLException {
        final var sql = """
            SELECT
                Text
            FROM
                Fact
            WHERE
                LinkID = ? AND
                LinkTableID = 5 AND
                FactTypeID = (SELECT FactType.ID FROM FactType WHERE FactType.Abbreviation = '_ID') AND
                Preferred = 1
            """;
        try (final var q = this.db.prepareStatement(sql)) {
            q.setLong(1, idPerson);
            try (final var rs = q.executeQuery()) {
                if (rs.next()) {
                    final var text = requireNonNullElse(rs.getString("Text"), "");
                    final var uuidExisting = asUuid(text);
                    final var isOK = uuidExisting.isPresent() && uuidExisting.get().equals(uuidOptimal);
                    if (isOK) {
                        LOG.debug("    existing Preferred _ID Fact is up-to-date; no action required.");
                    }
                    else {
                        LOG.warn("    incorrect value on existing Preferred _ID Fact: \"{}\" <=======================", text);
                    }
                    return isOK;
                }
            }
        }
        return false;
    }

    private int readSyncVersion() throws SQLException {
        final var sql = """
            SELECT
                CAST(StringValue AS INTEGER) AS SyncVersion
            FROM
                Setting
            WHERE
                Name = 'SyncVersion'
            """;
        try (final var q = this.db.prepareStatement(sql)) {
            try (final var rs = q.executeQuery()) {
                if (rs.next()) {
                    final var syncVersion = rs.getInt("SyncVersion");
                    LOG.debug("Current SyncVersion of FTM tree: {}", syncVersion);
                    return syncVersion;
                }
            }
        }
        return 0;
    }

    private long findFactType_ID() throws SQLException {
        final var sql = """
            SELECT
                ID, Name, ShortName, Abbreviation, Tag, SyncVersion
            FROM
                FactType
            WHERE
                Abbreviation = '_ID'
            """;
        try (final var q = this.db.prepareStatement(sql)) {
            try (final var rs = q.executeQuery()) {
                if (rs.next()) {
                    final var id = rs.getLong("ID");
                    final var name = requireNonNullElse(rs.getString("Name"), "");
                    final var shortName = requireNonNullElse(rs.getString("ShortName"), "");
                    final var abbreviation = requireNonNullElse(rs.getString("Abbreviation"), "");
                    final var tag = requireNonNullElse(rs.getString("Tag"), "");
                    final var syncVersion = rs.getInt("SyncVersion");
                    LOG.debug("existing FactType: ID={}, Name=\"{}\", ShortName=\"{}\", Abbreviation=\"{}\", Tag=\"{}\", SyncVersion={}", id, name, shortName, abbreviation, tag, syncVersion);

                    return id;
                }
            }
        }
        LOG.warn("Could not find FactType _ID.");
        return 0L;
    }

    private void createFactType(int syncVersion) throws SQLException {
        LOG.info("Creating new FactType _ID...");

        long maxID = -1L;
        try (final PreparedStatement select = db.prepareStatement("SELECT MAX(FactType.ID) AS maxID FROM FactType")) {
            try (final ResultSet rs = select.executeQuery()) {
                if (rs.next()) {
                    maxID = rs.getLong("maxID");
                }
            }
        }
        LOG.debug("Max FactType ID: {}", maxID);

        long seqID = -1L;
        try (final PreparedStatement select = db.prepareStatement("SELECT seq AS seqID FROM sqlite_sequence WHERE name = 'FactType'")) {
            try (final ResultSet rs = select.executeQuery()) {
                if (rs.next()) {
                    seqID = rs.getLong("seqID");
                }
            }
        }
        LOG.debug("FactType sequence value: {}", seqID);

        // Sanity check: since we are adding a FactType here, make sure the sequence and the primary key
        // are what we expect them to be. Otherwise, bail out.
        if (maxID < 0L || seqID < 0L || maxID != seqID) {
            LOG.error("Unexpected values for FactType primary key/sequence: FactType.ID={}, seq={}", maxID, seqID);
            throw new SQLException("Unexpected values for FactType primary key/sequence; will not update database");
        }

        if (maxID < 1000L) {
            // Special logic here, for case where no custom FactTypes at all exist in the database.
            // Note: FTM rigs custom FactTypes so their IDs are greater than or equal to 1001.
            LOG.warn("No custom FactTypes were found in the database; will update FactType ID sequence to 1000.");
            try (final PreparedStatement update = db.prepareStatement("UPDATE sqlite_sequence SET seq = 1000 WHERE name = 'FactType'")) {
                update.executeUpdate();
            }
        }

        try (final PreparedStatement insert = db.prepareStatement(
            "INSERT INTO FactType(Name, ShortName, Abbreviation, FactClass, Tag, SyncVersion) " +
                "VALUES('_ID','_ID','_ID',33025,'EVEN',?)")) {
            insert.setLong(1, syncVersion);
            insert.executeUpdate();
        }
    }

    //DO_UPDATE: set any/all _ID Preferred to 0, and INSERT our new one (_ID, OPTIMAL, Preferred) (also set SyncVersion in Fact and Person)
    private void doUpdate(final long idPerson, final long idFactType, final UUID optimal, final int syncVersion) throws SQLException {
        demoteExistingIds(idPerson, idFactType, syncVersion);
        insertOptimalUuid(idPerson, idFactType, optimal, syncVersion);
        updateSyncVersion(idPerson, syncVersion);
    }

    private void demoteExistingIds(long idPerson, long idFactType, int syncVersion) throws SQLException {
        final var sql = """
            UPDATE
                Fact
            SET
                Preferred = 0,
                SyncVersion = ?
            WHERE
                LinkID = ? AND
                LinkTableID = 5 AND
                FactTypeID = ?
            """;
        LOG.info("    "+sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"), syncVersion, idPerson, idFactType);
        if (!options.force) {
            LOG.warn("    THIS WAS A TEST; NO DATA HAS BEEN CHANGED! Use --force to force an update.");
        }
        else {
            try (final PreparedStatement insert = db.prepareStatement(sql)) {
                insert.setInt(1, syncVersion);
                insert.setLong(2, idPerson);
                insert.setLong(3, idFactType);
                final var cUpdate = insert.executeUpdate();
                LOG.info("    Updated row count: {}", cUpdate);
            }
        }
    }

    private void insertOptimalUuid(long idPerson, long idFactType, UUID optimal, int syncVersion) throws SQLException {
        final String sql = "INSERT INTO Fact(LinkID, LinkTableID, FactTypeID, Preferred, Text, SyncVersion) VALUES (?,5,?,1,?,?)";
        LOG.info("    "+sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"), idPerson, idFactType, optimal.toString(), syncVersion);
        if (!options.force) {
            LOG.warn("    THIS WAS A TEST; NO DATA HAS BEEN CHANGED! Use --force to force an update.");
        }
        else {
            try (final PreparedStatement insert = db.prepareStatement(sql)) {
                insert.setLong(1, idPerson);
                insert.setLong(2, idFactType);
                insert.setString(3, optimal.toString());
                insert.setInt(4, syncVersion);
                insert.executeUpdate();

                final ResultSet generatedKeys = insert.getGeneratedKeys();
                if (!generatedKeys.next()) {
                    LOG.error("Could not update internal ID");
                } else if (generatedKeys.next()) {
                    LOG.warn("Database returned multiple IDs when we only expected one.");
                }
            }
        }
    }

    private void updateSyncVersion(long idPerson, int syncVersion) throws SQLException {
        final String sql = "UPDATE Person SET SyncVersion = ? WHERE ID = ?";
        LOG.info("    "+sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"), syncVersion, idPerson);
        if (!options.force) {
            LOG.warn("    THIS WAS A TEST; NO DATA HAS BEEN CHANGED! Use --force to force an update.");
        }
        else {
            try (final PreparedStatement insert = db.prepareStatement(sql)) {
                insert.setLong(1, syncVersion);
                insert.setLong(2, idPerson);
                final var cUpdate = insert.executeUpdate();
                LOG.info("    Updated row count: {}", cUpdate);
            }
        }
    }

    private static Optional<UUID> asUuid(final String s) {
        try {
            return Optional.of(UUID.fromString(s));
        }
        catch (final Throwable e) {
            LOG.trace("ignoring error while decoding as UUID: {}", s, e);
            return Optional.empty();
        }
    }
}
