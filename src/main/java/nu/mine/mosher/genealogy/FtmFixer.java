package nu.mine.mosher.genealogy;

import ch.qos.logback.classic.*;
import nu.mine.mosher.gnopt.Gnopt;
import org.slf4j.*;
import org.slf4j.Logger;
import org.xml.sax.*;

import javax.xml.parsers.*;
import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static java.util.Objects.*;

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
    private static final Logger LOG = LoggerFactory.getLogger(FtmFixer.class);;
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

            ftmFixer.fixOptimalUuid();
            ftmFixer.verifyXml();
            ftmFixer.verifyDanglingRelationships();
        } catch (final Exception e) {
            LOG.error("Error processing {}", arg, e);
        }
    }

    // finds relationships having no parents and one or zero children
    // TODO find relationships with one parent and no children
    private void verifyDanglingRelationships() throws SQLException {
        final var sql = """
            SELECT
                C.PersonID AS person_id,
                C.RelationshipID AS rel_id,
                COUNT(*) AS c_children
            FROM
                ChildRelationship C
            WHERE
                C.RelationshipID IN (
                    SELECT
                        R.ID
                    FROM
                        Relationship R
                    WHERE
                        R.Person1ID IS NULL AND
                        R.Person2ID IS NULL
                )
            GROUP BY
                C.RelationshipID
            HAVING
                c_children <= 1
            """;
        try (final var q = this.db.prepareStatement(sql); final var rs = q.executeQuery()) {
            while (rs.next()) {
                final var person = rs.getLong("person_id");
                LOG.warn("DANGLING RELATIONSHIP related to person ID: {}", person);
            }
        }
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
                    LOG.warn("INVALID XML found for citation starting with '<':");
                    LOG.warn("TITLE: {}", title);
                    LOG.warn("PERSON: {}", person);
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

        try (final PreparedStatement insert = db.prepareStatement("INSERT INTO FactType(Name, ShortName, Abbreviation, FactClass, Tag, SyncVersion) " + "VALUES('_ID','_ID','_ID',33025,'EVEN',?)")) {
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
