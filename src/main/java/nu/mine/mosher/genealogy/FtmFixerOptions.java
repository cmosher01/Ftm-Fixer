package nu.mine.mosher.genealogy;

import java.util.*;

public class FtmFixerOptions {
    public boolean force;
    public boolean verbose;
    public List<String> files = new ArrayList<>();



    public void help(final Optional<String> bool) {
        System.out.println("Usage:");
        System.out.println("    ftm-fixer [OPTIONS] <tree>.ftm [...]");
        System.out.println("Options:");
        System.out.println("    --help    Prints this help message.");
        System.out.println("    --force   Updates the database (otherwise only prints");
        System.out.println("              a log of what would be changed.");
        System.out.println("    --verbose Prints verbose log messages.");
    }

    public void force(final Optional<String> bool) {
        this.force = parseBool(bool);
    }

    public void f(final Optional<String> bool) {
        force(bool);
    }

    public void verbose(final Optional<String> bool) {
        this.verbose = parseBool(bool);
    }

    public void v(final Optional<String> bool) {
        verbose(bool);
    }



    public void __(final Optional<String> file) {
        this.files.add(file.get());
    }



    private static boolean parseBool(Optional<String> bool) {
        boolean v = true;
        if (bool.isPresent()) {
            v = Boolean.parseBoolean(bool.get());
        }
        return v;
    }
}
