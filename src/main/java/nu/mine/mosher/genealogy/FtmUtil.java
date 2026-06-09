package nu.mine.mosher.genealogy;

import org.slf4j.*;

import java.sql.*;
import java.util.*;

public class FtmUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FtmUtil.class);

    public static Optional<UUID> asUuid(final String s) {
        if (Objects.isNull(s) || s.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(UUID.fromString(s));
        }
        catch (final Throwable e) {
            LOG.info("ignoring error while decoding as UUID: {}", s, e);
            return Optional.empty();
        }
    }


    public static int getIntFrom(final String col, final ResultSet rs) throws SQLException {
        final var s = getStringFrom(col, rs);
        if (s.isBlank()) {
            return -1;
        }
        try {
            return Integer.parseInt(s, 10);
        } catch (final Exception ignore) {
            return -1;
        }
    }

    public static long getLongFrom(final String col, final ResultSet rs) throws SQLException {
        final var s = getStringFrom(col, rs);
        if (s.isBlank()) {
            return -1L;
        }
        try {
            return Long.parseLong(s, 10);
        } catch (final Exception ignore) {
            return -1L;
        }
    }

    public static Optional<FtmPlace> getPlaceFrom(final String col, final ResultSet rs) throws SQLException {
        final var factplace = getStringFrom(col, rs);
        if (factplace.isBlank() || rs.wasNull()) {
            return Optional.empty();
        }
        return Optional.of(FtmPlace.fromFtmPlace(factplace));
    }

    public static String getDateFrom(final String col, ResultSet rs) throws SQLException {
        final var factdate = getStringFrom(col, rs);
        if (factdate.isBlank()) {
            return "";
        }
        return FtmDate.fromFtmFactDate(factdate).toString();
    }

    public static String getStringFrom(final String col, final ResultSet rs) throws SQLException {
        final var s = rs.getString(col);
        if (rs.wasNull()) {
            return "";
        }
        if (s.isBlank()) {
            return "";
        }
        return s.strip();
    }
}
