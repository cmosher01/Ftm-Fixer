package nu.mine.mosher.genealogy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNullElse;

/**
 * Updates any necessary Person identification value.
 * <p>
 * To determine the OPTIMAL identification value for a Person, choose from the
 * following Facts, by this order of preference:
 * <p>
 * 1. _UID (FactType.Abbreviation)
 * 2. UUID (FactType.Abbreviation)
 * 3. GUID (FactType.Abbreviation)
 * 4. ID (FactType.Abbreviation)
 * 5. REFN (FactType.Tag)
 * 6. Person.PersonGUID
 * <p>
 * For any of the Facts 1-5 in this list, there could be multiple for a given person,
 * with one and only one of them being marked as Preferred, and we will choose that one.
 * FTM enforces the one-and-only-one-preferred rule, we assume; if by chance there are
 * more than one such records, then we will choose an arbitrary one. If there are no
 * preferred records (per type), then we choose an arbitrary one (in order of preference).
 * <p>
 * If none exist (which should never happen, since FTM always provides a PersonGUID),
 * we generate a new random UUID.
 * <p>
 * This results in exactly one OPTIMAL identification UUID for the given Person.
 * <p>
 * If there is exactly one _UID record marked as Preferred, and it already has the OPTIMAL
 * identification as its value, then no update is necessary, so do nothing.
 * <p>
 * Otherwise, update any existing _UID records to mark them as NOT Preferred; then
 * insert one new _UID record, marked Preferred, with the OPTIMAL identification value
 * (as a string in standard UUID format).
 * <p>
 * The end result is intended to be as follows:
 * 1. Leave any existing UUID, GUID, ID, REFN, or PersonGUID values completely untouched.
 * 2. Do not remove any existing _UID values.
 * 3. There will be exactly one _UID value marked as Preferred, and
 * 4. it will have as its value the preferred UUID (OPTIMAL identification for the Person)
 */
public class FtmFixer
{
    private static final Logger LOG = LoggerFactory.getLogger(FtmFixer.class);

    private Connection db;
    private boolean dryRun = false;

    private FtmFixer(final Connection db)
    {
        this.db = db;
    }

    public static void main(final String... args) throws SQLException
    {
        if (args.length < 1)
        {
            LOG.error("Missing required argument: <tree>.ftm");
            System.exit(1);
        }

        final var path = Paths.get(args[0]);
        LOG.debug("opening FTM tree file: {}", path);
        final var ftmFixer = new FtmFixer(DriverManager.getConnection("jdbc:sqlite:" + path));

        ftmFixer.reportPersons();

        LOG.debug("Program completed normally.");
    }

    private void reportPersons() throws SQLException
    {
        final var syncVersion = readSyncVersion();

        if (findFactType_ID() == 0L && !dryRun)
        {
            createFactType(syncVersion);
            if (findFactType_ID() == 0L)
            {
                LOG.error("Could not create FactType _ID record.");
            }
        }

        final var idFactType = findFactType_ID();

        final var sql = "SELECT ID, FullName FROM Person";
        try (final var q = this.db.prepareStatement(sql); final var rs = q.executeQuery())
        {
            while (rs.next())
            {
                final var idPerson = rs.getLong("ID");
                final var name = requireNonNullElse(rs.getString("FullName"), "");
                LOG.info("Person: ID={}, FullName={}", idPerson, name);

                final var optimal = findOptimalUuid(idPerson);

                // if C != 1 then DO_UPDATE, else
                // if Text exactly equals OPTIMAL, then do nothing, else DO_UPDATE
                if (!isExactlyOnePreferred_ID(idPerson) || !isMatching_ID(idPerson, optimal))
                {
                    doUpdate(idPerson, idFactType, optimal, syncVersion);
                }




                // NOTE: for any program that is trying to locate a person based on a UUID, it should check
                // all values (as in the first query) for any matching UUID. This will allow for permalinks, including
                // handling merging of persons (where the merged person would contain both UUIDs).
            }
        }
    }

    private UUID findOptimalUuid(final long idPerson) throws SQLException
    {
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

        try (final var q = this.db.prepareStatement(sql))
        {
            q.setLong(1, idPerson);
            q.setLong(2, idPerson);
            try (final var rs = q.executeQuery())
            {
                final var header = String.format("    %7s %4s %4s %1s %-40s %1s %5s", "Fact ID", "Tag", "Abrv", "P", "Text", "U", "SyncVersion");
                LOG.info(header);
                while (rs.next())
                {
                    final var idFact = rs.getLong("ID");
                    final var tag = requireNonNullElse(rs.getString("Tag"), "");
                    final var abbreviation = requireNonNullElse(rs.getString("Abbreviation"), "");
                    final var preferred = rs.getInt("Preferred") != 0;
                    final var text = requireNonNullElse(rs.getString("Text"), "");
                    final var syncVersion = rs.getInt("SyncVersion");
                    final var optUuid = asUuid(text);

                    final var msg = String.format("    %7d %4s %4s %c %-40s %c %5d", idFact, tag, abbreviation, preferred ? '*' : ' ', text, optUuid.isPresent() ? '*' : ' ', syncVersion);
                    LOG.info(msg);

                    if (optUuid.isPresent() && optimal.isEmpty())
                    {
                        optimal = optUuid;
                    }
                }
            }
        }

        if (optimal.isEmpty())
        {
            optimal = Optional.of(UUID.randomUUID());
            LOG.warn("    Could not find any valid UUID, so one was generated: {}", optimal.get());
        }

        LOG.info("    OPTIMAL UUID: ----> {} <----------------", optimal.get());

        return optimal.get();
    }

    private boolean isExactlyOnePreferred_ID(final long idPerson) throws SQLException
    {
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

        try (final var q = this.db.prepareStatement(sql))
        {
            q.setLong(1, idPerson);
            try (final var rs = q.executeQuery())
            {
                if (rs.next())
                {
                    final var c = rs.getInt("C");
                    if (c == 1)
                    {
                        LOG.info("    count of existing Preferred _ID Facts: {}", c);
                    }
                    else
                    {
                        LOG.warn("    count of existing Preferred _ID Facts: {} <=======================", c);
                    }
                    return c == 1;
                }
            }
        }

        return false;
    }

    private boolean isMatching_ID(final long idPerson, final UUID uuidOptimal) throws SQLException
    {
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
        try (final var q = this.db.prepareStatement(sql))
        {
            q.setLong(1, idPerson);
            try (final var rs = q.executeQuery())
            {
                if (rs.next())
                {
                    final var text = requireNonNullElse(rs.getString("Text"), "");
                    final var uuidExisting = asUuid(text);
                    final var isOK = uuidExisting.isPresent() && uuidExisting.get().equals(uuidOptimal);
                    if (isOK)
                    {
                        LOG.info("    existing Preferred _ID Fact is up-to-date; no action required.");
                    }
                    else
                    {
                        LOG.warn("    incorrect value on existing Preferred _ID Fact: \"{}\" <=======================", text);
                    }
                    return isOK;
                }
            }
        }
        return false;
    }

    private int readSyncVersion() throws SQLException
    {
        final var sql = """
            SELECT
                CAST(StringValue AS INTEGER) AS SyncVersion
            FROM
                Setting
            WHERE
                Name = 'SyncVersion'
            """;
        try (final var q = this.db.prepareStatement(sql))
        {
            try (final var rs = q.executeQuery())
            {
                if (rs.next())
                {
                    final var syncVersion = rs.getInt("SyncVersion");
                    LOG.info("Current SyncVersion of FTM tree: {}", syncVersion);
                    return syncVersion;
                }
            }
        }
        return 0;
    }

    private long findFactType_ID() throws SQLException
    {
        final var sql = """
            SELECT
                ID, Name, ShortName, Abbreviation, Tag, SyncVersion
            FROM
                FactType
            WHERE
                Abbreviation = '_ID'
            """;
        try (final var q = this.db.prepareStatement(sql))
        {
            try (final var rs = q.executeQuery())
            {
                if (rs.next())
                {
                    final var id = rs.getLong("ID");
                    final var name = requireNonNullElse(rs.getString("Name"), "");
                    final var shortName = requireNonNullElse(rs.getString("ShortName"), "");
                    final var abbreviation = requireNonNullElse(rs.getString("Abbreviation"), "");
                    final var tag = requireNonNullElse(rs.getString("Tag"), "");
                    final var syncVersion = rs.getInt("SyncVersion");
                    LOG.info("existing FactType: ID={}, Name=\"{}\", ShortName=\"{}\", Abbreviation=\"{}\", Tag=\"{}\", SyncVersion={}",
                        id, name, shortName, abbreviation, tag, syncVersion);

                    return id;
                }
            }
        }
        LOG.warn("Could not find FactType _ID.");
        return 0L;
    }

    private void createFactType(int syncVersion) throws SQLException
    {
        LOG.info("Creating new FactType _ID...");

        long maxID = -1L;
        try (final PreparedStatement select = db.prepareStatement(
            "SELECT MAX(FactType.ID) AS maxID FROM FactType")) {
            try (final ResultSet rs = select.executeQuery()) {
                if (rs.next()) {
                    maxID = rs.getLong("maxID");
                }
            }
        }
        LOG.debug("Max FactType ID: {}", maxID);

        long seqID = -1L;
        try (final PreparedStatement select = db.prepareStatement(
            "SELECT seq AS seqID FROM sqlite_sequence WHERE name = 'FactType'")) {
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
            try (final PreparedStatement update = db.prepareStatement(
                "UPDATE sqlite_sequence SET seq = 1000 WHERE name = 'FactType'")) {
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

    private void doUpdate(final long idPerson, final long idFactType, final UUID optimal, final int syncVersion) throws SQLException
    {
        //DO_UPDATE: set any/all _ID Preferred to 0, and INSERT our new one (_ID, OPTIMAL, Preferred) (also set SyncVersion in Fact and Person)

        {
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
            LOG.info(sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"),
                syncVersion, idPerson, idFactType);
            if (dryRun) {
                LOG.info("DRY RUN; no data changed. Use -f to force an update.");
            }
            else
            {
                try (final PreparedStatement insert = db.prepareStatement(sql))
                {
                    insert.setInt(1, syncVersion);
                    insert.setLong(2, idPerson);
                    insert.setLong(3, idFactType);
                    final var cUpdate = insert.executeUpdate();
                    LOG.info("Updated row count: {}", cUpdate);
                }
            }
        }

        {
            final String sql = "INSERT INTO Fact(LinkID, LinkTableID, FactTypeID, Preferred, Text, SyncVersion) VALUES (?,5,?,1,?,?)";
            LOG.info(sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"),
                idPerson, idFactType, optimal.toString(), syncVersion);
            if (dryRun) {
                LOG.info("DRY RUN; no data changed. Use -f to force an update.");
            } else
            {
                try (final PreparedStatement insert = db.prepareStatement(sql))
                {
                    insert.setLong(1, idPerson);
                    insert.setLong(2, idFactType);
                    insert.setString(3, optimal.toString());
                    insert.setInt(4, syncVersion);
                    insert.executeUpdate();

                    final ResultSet generatedKeys = insert.getGeneratedKeys();
                    if (!generatedKeys.next())
                    {
                        LOG.error("Could not update internal ID");
                        return;
                    }
                    if (generatedKeys.next())
                    {
                        LOG.warn("Database returned multiple IDs when we only expected one.");
                    }
                }
            }
        }

        {
            final String sql = "UPDATE Person SET SyncVersion = ? WHERE ID = ?";
            LOG.info(sql.replaceAll("(?U)\\s+", " ").replaceAll("\\?", "{}"),
                syncVersion, idPerson);
            if (dryRun) {
                LOG.info("DRY RUN; no data changed. Use -f to force an update.");
            } else
            {
                try (final PreparedStatement insert = db.prepareStatement(sql))
                {
                    insert.setLong(1, syncVersion);
                    insert.setLong(2, idPerson);
                    final var cUpdate = insert.executeUpdate();
                    LOG.info("Updated row count: {}", cUpdate);
                }
            }
        }
    }



    private static Optional<UUID> asUuid(final String s)
    {
        try
        {
            return Optional.of(UUID.fromString(s));
        }
        catch (final Throwable e)
        {
            LOG.trace("ignoring error while decoding as UUID: {}", s, e);
            return Optional.empty();
        }
    }
}