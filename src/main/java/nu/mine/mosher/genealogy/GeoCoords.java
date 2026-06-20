package nu.mine.mosher.genealogy;

//import org.apache.hc.core5.net.URIBuilder;
import org.slf4j.*;

//import java.net.URL;
import java.util.Optional;

// from ftm-web-view (with some things commented out that we don't need here)

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public record GeoCoords(
    double radiansLatitude,
    double radiansLongitude,
    double degreesLatitude,
    double degreesLongitude
) {
    private static final Logger LOG = LoggerFactory.getLogger(GeoCoords.class);

    public static Optional<GeoCoords> parse(final String radLat, final String radLon) {
        final Optional<Double> odrLat = parseRadians(radLat);
        if (odrLat.isEmpty()) {
            return Optional.empty();
        }
        final Optional<Double> oddLat = degreesFromRadians(odrLat);
        if (oddLat.isEmpty()) {
            return Optional.empty();
        }
        final Optional<Double> odrLon = parseRadians(radLon);
        if (odrLon.isEmpty()) {
            return Optional.empty();
        }
        final Optional<Double> oddLon = degreesFromRadians(odrLon);
        if (oddLon.isEmpty()) {
            return Optional.empty();
        }

//        final Optional<URL> ou = buildUrl(oddLat, oddLon);
//        if (ou.isEmpty()) {
//            return Optional.empty();
//        }

        return Optional.of(new GeoCoords(odrLat.get(), odrLon.get(), oddLat.get(), oddLon.get()));
    }

//    private static Optional<URL> buildUrl(Optional<Double> oddLat, Optional<Double> oddLon) {
//        /*
//         *      https://www.google.com/maps/search/?api=1&query=-33.712206,150.311941
//         */
//
//        try {
//            final String pair = String.format("%f,%f", oddLat.get(), oddLon.get());
//            return Optional.of(
//                new URIBuilder().
//                setScheme("https").
//                setHost("www.google.com").
//                setPathSegments("maps", "search", "").
//                setParameter("api", "1").
//                setParameter("query", pair).
//                build().
//                toURL());
//        } catch (final Throwable e) {
//            LOG.warn("Tried to build invalid URL", e);
//        }
//        return Optional.empty();
//    }

    private static Optional<Double> degreesFromRadians(final Optional<Double> radians) {
        if (radians.isPresent()) {
            final double r = radians.get();
            final double d = Math.toDegrees(r);
            return Optional.of(d);
        } else {
            return Optional.empty();
        }
    }
    private static Optional<Double> parseRadians(final String s) {
        if (s.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Double.parseDouble(s));
        } catch (final Throwable e) {
            LOG.warn("Invalid number format for geographic coordinate: {}", s, e);
            return Optional.empty();
        }
    }
}
