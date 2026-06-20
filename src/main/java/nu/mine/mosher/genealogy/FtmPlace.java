package nu.mine.mosher.genealogy;


import org.slf4j.*;
import org.w3c.dom.Element;

import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;

//import static nu.mine.mosher.genealogy.StringUtils.safe;
//import static nu.mine.mosher.genealogy.XmlUtils.e;


// from ftm-web-view (with some things commented out that we don't need here)

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class FtmPlace {
    private static final Logger LOG =  LoggerFactory.getLogger(FtmPlace.class);

    private final List<String> hierarchy;
    private final String description;
    private final boolean resolved;

    private final Optional<GeoCoords> coords;
    private final boolean neg; // TODO what is this flag for?
    private final int codeStandard; // TODO what place-coding standard is this?

    private String abbreviatedOverride;
    private boolean ditto;

    private FtmPlace(final List<String> hierarchy, final String description, final boolean resolved, final Optional<GeoCoords> coords, final boolean neg, final int codeStandard) {
        this.hierarchy = hierarchy;
        this.description = description;
        this.coords = coords;
        this.neg = neg;
        this.codeStandard = codeStandard;
        this.abbreviatedOverride = "";
        this.resolved = resolved;
    }

    @Override
    public String toString() {
        return "FtmPlace{" +
            "hierarchy=[" + dumpHierarchy() + ']' +
            ", description=\"" + description + '\"' +
            ", codeStandard=" + codeStandard +
            ", coords=" + coords +
            '}';
    }

    private String dumpHierarchy() {
        return
            this.
            hierarchy.
            stream().
            map(p -> "\""+p+"\"").
            collect(Collectors.joining(","));
    }

    public List<String> getHierarchy() {
        return new ArrayList<>(this.hierarchy);
    }

    public String description() {
        return this.description;
    }

    public static FtmPlace fromFtmPlace(final String s) {
        final FtmPlace place = new Builder(s).build();
        LOG.debug("FtmPlace=\"{}\" --> \"{}\"", s, place);
        return place;
    }

    public static FtmPlace empty() {
        return new FtmPlace(new ArrayList<>(), "", false, Optional.empty(), false, 0);
    }

//    public boolean isBlank() {
//        return !this.ditto && /*this.sDisplay.isBlank() &&*/ safe(this.description).isBlank();
//    }
//
//    public void appendTo(final Element parent) {
//        if (this.ditto) {
//            parent.setTextContent("\u00A0\u3003");
//        } else {
//            final Element name = e(parent, "span");
//            name.setTextContent(this.abbreviatedOverride.isBlank() ? this.description : this.abbreviatedOverride);
//
//            if (this.coords.isPresent()) {
//                final Element sup = e(parent, "sup");
//                final Element google = e(sup, "a");
//                google.setAttribute("href", this.coords.get().urlGoogleMaps().toExternalForm());
//                google.setTextContent(new String(Character.toChars(0x1F30D)));
//            }
//        }
//    }

    public void setAbbreviatedOverride(final List<String> parts) {
        this.abbreviatedOverride = String.join(", ", parts);
    }

    public boolean isResolved() {
        return this.resolved;
    }

    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof FtmPlace that)) {
            return false;
        }
        return this.hierarchy.equals(that.hierarchy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.hierarchy);
    }

    public void setDitto() {
        this.ditto = true;
    }












    private static class Builder {
        private String description;
        private Optional<GeoCoords> coords = Optional.empty();
        private boolean neg;
        private int codeStandard;
        private final List<String> hierarchy = new ArrayList<>(5);
        private boolean resolved;

        public Builder(final String description) {
            if (Objects.isNull(description) || description.isBlank()) {
                this.description = "";
            } else {
                // default value if any parsing fails:
                this.description = String.format("\u201C%s\u201D", description);
                // try to parse
                try {
                    parseDescription(description);
                } catch (final Throwable e) {
                    LOG.warn("unknown place name format for {}", description, e);
                }
            }
        }

        public FtmPlace build() {
            return new FtmPlace(hierarchy, description, resolved, coords, neg, codeStandard);
        }


        /*
        Family Tree Maker does some encoding within the place name column.

        There are two main types of place names.

        Both types start with a slash.

        Both types end with

               ... [CODE] / [LATITUDE] / [LONGITUDE]

        where CODE could be negative (which is obviously a flag that represents something), and
        LATITUDE/LONGITUDE are coordinates, in radians.

        Immediately preceding this will be either a slash or a vertical bar, which
        is used as an indicator of the main type of the place name, and dictates the
        format of the rest of the string preceding that:


        "/" Slashed place names are in this format:

            / [p0] / [p1] / [p2] / [p3] / [p4] / ...

        where p0 through p4 are place names in a hierarchy.
        The parts of "resolved" places are at the end.


        "|" Vertical bar place names are in this format:

            / [name] | ...

         */


        /*
                /Hamilton, Madison, New York, USA|/0.7474722/-1.318502
                /Place, Name w/some slash/es | and, vertical | bars|//
         */
        private static final Pattern FTM_PLACE_WITH_VERTICALBAR = Pattern.compile("^/(?<name>.*)\\|(?<code>[^/|]*?)/(?<lat>[^/|]*?)/(?<lon>[^/|]*?)$");

        /*
                /Room 401, Flint Hall, Syracuse University/Syracuse/Onondaga/New York/USA/11269/0.7513314/-1.329023
                /another place / with slashes | and  bars, but, resolved, in///Connecticut/USA/-9//

            Use the first capture group ("name") from the first pattern as input to the second pattern:
        */
        private static final Pattern FTM_PLACE_WITH_SLASH = Pattern.compile("^/(?<name>.*)/(?<code>[^/|]*?)/(?<lat>[^/|]*?)/(?<lon>[^/|]*?)$");

        private static final Pattern FTM_PLACE_HIERARCHICAL = Pattern.compile("^(?<p0>.*)/(?<p1>[^/|]*?)/(?<p2>[^/|]*?)/(?<p3>[^/|]*?)/(?<p4>[^/|]*?)$");


        private void parseDescription(final String description) {
            //-------------------------------------------------------
            {
                final Matcher withBar = FTM_PLACE_WITH_VERTICALBAR.matcher(description);
                if (withBar.matches()) {
                    setCoords(withBar.group("lat"), withBar.group("lon"));
                    setCode(withBar.group("code"));

                    parseAndAddHierarchy(withBar.group("name"));

                    buildDescription();
                    return;
                }
            }
            //-------------------------------------------------------
            {
                final Matcher withSlash = FTM_PLACE_WITH_SLASH.matcher(description);
                if (withSlash.matches()) {
                    setCoords(withSlash.group("lat"), withSlash.group("lon"));
                    setCode(withSlash.group("code"));

                    this.resolved = true;
                    final Matcher hier = FTM_PLACE_HIERARCHICAL.matcher(withSlash.group("name"));
                    if (hier.matches()) {
                        parseAndAddHierarchy(hier.group("p0"));
                        addHierarchy(hier.group("p1"));
                        addHierarchy(hier.group("p2"));
                        addHierarchy(hier.group("p3"));
                        addHierarchy(hier.group("p4"));
                    } else {
                        parseAndAddHierarchy(withSlash.group("name"));
                    }

                    buildDescription();
                    return;
                }
            }
            //-------------------------------------------------------
        }

        private void buildDescription() {
            this.description = String.join(", ", this.hierarchy);
        }

        private void parseAndAddHierarchy(final String csvParts) {
            Arrays.stream(csvParts.split(",")).
                map(String::trim).
                forEach(this::addHierarchy);
        }

        private void addHierarchy(final String part) {
            if (!part.isBlank()) {
                this.hierarchy.add(part);
            }
        }

        private void setCoords(final String lat, final String lon) {
            this.coords = GeoCoords.parse(lat, lon);
        }

        private void setCode(final String code) {
            this.codeStandard = parseCode(code);

            this.neg = false;
            if (this.codeStandard < 0) {
                this.neg = true;
                this.codeStandard = -this.codeStandard;
            }
        }

        private static int parseCode(final String s) {
            if (s.isBlank()) {
                return 0;
            }
            try {
                return Integer.parseInt(s);
            } catch (final Throwable e) {
                LOG.warn("Unknown format for FTM place code: {}", s, e);
                return 0;
            }
        }
    }
}
