import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

import static java.util.Date.parse;

public class Main {
    static Connection conn = null;
    static String lastProcessedPath = "";
    static int counterAll = 0;
    private static String Newsgroups = "";
    private static String From = "";
    private static String To = "";
    private static String Path = "";
    private static String Dater = "";
    private static String DateReceived = "";
    private static String Expires = "";
    private static String ArticleID = "";
    private static String Subject = "";
    private static String MessageID = "";
    private static String Organization = "";
    private static String References = "";
    private static String Xref = "";
    private static String XRefs = "";
    private static String FilePath = "";
    private static Boolean keepGoing = false;
    private static int Lines = 0;
    private static String header = "";
    private static String body = "";

    public static void main(String[] args) {
        System.out.println("Starting!");
        String db_Server = args[0];
        String database_name = args[1];

        // CONNECT TO MYSQL
        final String USERNAME = args[2];
        final String PASSWORD = args[3];
        final String CONN_STRING = "jdbc:mysql://" + db_Server + ":3306/" + database_name;
        final String dir = args[4];

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(CONN_STRING, USERNAME, PASSWORD);
            System.out.println("Database Connected!");
        } catch (SQLException e) {
            System.err.println(e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        try {
            PreparedStatement rowStatement = null;
            rowStatement = conn.prepareStatement("SELECT FilePath FROM archive order by id DESC limit 1");
            ResultSet lastProcessed = rowStatement.executeQuery();
            lastProcessed.next();
            lastProcessedPath = lastProcessed.getString(1);
        } catch (SQLException e) {
            //e.printStackTrace();
        }

        System.out.println("Database last processed path: " + lastProcessedPath);
        System.out.println("Processing... get a coffee!");


        try {
            recusiveList(dir);
        } catch (IOException e) {
            e.printStackTrace();
        }


        // System.exit(0);


    }

    private static void recusiveList(String absolutePath) throws IOException {
        File f = new File(absolutePath);
        File[] fl = f.listFiles();
        boolean stopProcessing = false;


        for (int i = 0; i < fl.length; i++)
            if (fl[i].isDirectory() && !fl[i].isHidden()) {
                FilePath = fl[i].getAbsolutePath().replace("C:\\Users\\jaros\\Videos\\utzoo-wiseman-usenet-archive\\", "");
                System.out.println(counterAll + " - " + FilePath);

                recusiveList(fl[i].getAbsolutePath());
            } else {
                counterAll++;

                FilePath = fl[i].getAbsolutePath().replace("C:\\Users\\jaros\\Videos\\utzoo-wiseman-usenet-archive\\", "");

                if (FilePath.equals(lastProcessedPath) == true) {
                    keepGoing = true;
                }

                if (lastProcessedPath.equals("") == true) {
                    keepGoing = true;
                }

                if (keepGoing == true) {

                    if (FilePath.endsWith(".") == false) {
                        //System.out.println(counterAll + " - " + fl[i].getAbsolutePath());
                        // System.exit(0);


                        // System.out.println(counterAll + " - " + FilePath); //  fl[i].getName()


                        try (BufferedReader br = new BufferedReader(new FileReader(fl[i].getAbsolutePath()))) {

                            Boolean linebreak = false;
                            header = "";
                            body = "";
                            Newsgroups = "";
                            From = "";
                            Path = "";
                            Dater = "";
                            DateReceived = "";
                            Expires = "";
                            ArticleID = "";
                            Subject = "";
                            MessageID = "";
                            Organization = "";
                            References = "";
                            Xref = "";
                            XRefs = "";
                            Lines = 0;


                            for (String line; (line = br.readLine()) != null; ) {


                                if ((line.length() == 0) || (line == null) || (line.equals(" ") == true) || (line.equals("") == true)) {
                                    linebreak = true;
                                }

                                if (linebreak == false) {
                                    header = header + line + "\r\n";
                                } else {
                                    body = body + line + "\r\n";
                                }


                                boolean isFound = line.contains(":");
                                if (isFound) {
                                    String[] splitLine = line.split(":", 2);
//System.out.println(splitLine[0].trim() + " | " + splitLine[1].trim());


                                    if (Newsgroups.length() == 0) {
                                        if (splitLine[0].trim().equals("Newsgroups") == true) Newsgroups = splitLine[1].trim();
                                    }
                                    if (From.length() == 0) {
                                        if (splitLine[0].trim().equals("From") == true) From = splitLine[1].trim();
                                    }
                                    if (To.length() == 0) {
                                        if (splitLine[0].trim().equals("To") == true) To = splitLine[1].trim();
                                    }
                                    if (Path.length() == 0) {
                                        if (splitLine[0].trim().equals("Path") == true) Path = splitLine[1].trim();
                                    }
                                    if (Dater.length() == 0) {
                                        if (splitLine[0].trim().equals("Date") == true) Dater = splitLine[1].trim();
                                    }
                                    if (DateReceived.length() == 0) {
                                        if (splitLine[0].trim().equals("Date-Received") == true) DateReceived = splitLine[1].trim();
                                    }
                                    if (Expires.length() == 0) {
                                        if (splitLine[0].trim().equals("Expires") == true) Expires = splitLine[1].trim();
                                    }
                                    if (ArticleID.length() == 0) {
                                        if (splitLine[0].trim().equals("Article-I.D.") == true) ArticleID = splitLine[1].trim();
                                    }
                                    if (Subject.length() == 0) {
                                        if (splitLine[0].trim().equals("Subject") == true) Subject = splitLine[1].trim();
                                    }
                                    if (MessageID.length() == 0) {
                                        if (splitLine[0].trim().equals("Message-ID") == true) MessageID = splitLine[1].trim();
                                    }
                                    if (Organization.length() == 0) {
                                        if (splitLine[0].trim().equals("Organization") == true) Organization = splitLine[1].trim();
                                    }
                                    if (References.length() == 0) {
                                        if (splitLine[0].trim().equals("References") == true) References = splitLine[1].trim();
                                    }
                                    if (Xref.length() == 0) {
                                        if (splitLine[0].trim().equals("Xref") == true) Xref = splitLine[1].trim();
                                    }
                                    if (XRefs.length() == 0) {
                                        if (splitLine[0].trim().equals("X-Refs") == true) XRefs = splitLine[1].trim();
                                    }

                                    if (Lines == 0) {

                                        try {
                                            if (splitLine[0].trim().equals("Lines") == true) Lines = Integer.parseInt(splitLine[1].trim());
                                        } catch (NumberFormatException e) {
                                            Lines = 0;
                                            e.printStackTrace();
                                        }

                                    }
                                /*
                            if (header.length() == 0) {
                                if (splitLine[0].trim().equals("header") == true) header = splitLine[1].trim();
                            }
                            if (body.length() == 0) {
                                if (splitLine[0].trim().equals("body") == true) body = splitLine[1].trim();
                            }
*/
                                    splitLine[0] = "";
                                    splitLine[1] = "";


                                }


                            }

                            try {
                                if (Newsgroups.length() > 0) {


                                    int alreadyInDB = 0;
                                    try {
                                        PreparedStatement statementor = null;
                                        statementor = conn.prepareStatement("SELECT count(*) FROM archive WHERE `Message-ID`= '" + MessageID + "'");
                                        ResultSet resultSet = statementor.executeQuery();
                                        resultSet.next();
                                        alreadyInDB = resultSet.getInt(1);
                                    } catch (SQLException e) {
                                        alreadyInDB = 0;
                                    }

                                    if (alreadyInDB != 1) {
                                        alreadyInDB = 0;
                                    }

                                    if (alreadyInDB == 0) {
                                        PreparedStatement preparedStatement = null;
                                        preparedStatement = conn.prepareStatement("insert into `usenetarchives`.`archive` (`Newsgroups`, `From`, `To`, `Path`, `Date`, `Date-Received`, `DateParsed`, `Expires`, `Article-I.D.`, `Subject`, `Message-ID`, `Organization`, `References`, `Xref`, `X-Refs`, `Lines`, `FilePath`,`header`,`body`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

                                        try {
                                            preparedStatement.setString(1, Newsgroups);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(2, From);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(3, To);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(4, Path);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(5, Dater);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(6, DateReceived);
                                        } catch (Exception e1) {
                                        }


                                        try {
                                            Long dateParsed = parse(Dater);
                                            preparedStatement.setString(7, dateParsed.toString());
                                        } catch (Exception e1) {
                                            preparedStatement.setString(7, "");
                                        }

                                        try {
                                            preparedStatement.setString(8, Expires);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(9, ArticleID);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(10, Subject);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(11, MessageID);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(12, Organization);
                                        } catch (Exception e1) {
                                        }

                                        try {
                                            preparedStatement.setString(13, References);
                                        } catch (Exception e1) {
                                        }


                                        try {
                                            preparedStatement.setString(14, Xref);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(15, XRefs);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(16, String.valueOf(Lines));
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(17, FilePath);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(18, header);
                                        } catch (Exception e1) {
                                        }
                                        try {
                                            preparedStatement.setString(19, body);
                                        } catch (Exception e1) {
                                        }


                                        try {
                                            preparedStatement.executeUpdate();
                                            preparedStatement.clearParameters();
                                            preparedStatement.close();
                                        } catch (SQLException e) {
                                            e.printStackTrace();
                                        }
                                    }

                                }

                            } catch (Exception e1) {
                                e1.printStackTrace();
                            }

                        }


                    }
                }
            }

    }
}

/*
CREATE TABLE `archive` (
	`id` INT(20) NOT NULL AUTO_INCREMENT,
	`Newsgroups` MEDIUMTEXT NULL DEFAULT NULL,
	`From` VARCHAR(255) NULL DEFAULT NULL,
	`Subject` VARCHAR(255) NULL DEFAULT NULL,
	`Date` VARCHAR(255) NULL DEFAULT NULL,
	`DateParsed` VARCHAR(255) NULL DEFAULT NULL,
	`Message-ID` VARCHAR(255) NULL DEFAULT NULL,
	`To` VARCHAR(255) NULL DEFAULT NULL,
	`Path` MEDIUMTEXT NULL,
	`Date-Received` VARCHAR(255) NULL DEFAULT NULL,
	`Expires` VARCHAR(255) NULL DEFAULT NULL,
	`Article-I.D.` VARCHAR(255) NULL DEFAULT NULL,
	`Organization` VARCHAR(255) NULL DEFAULT NULL,
	`References` MEDIUMTEXT NULL,
	`Xref` MEDIUMTEXT NULL,
	`X-Refs` MEDIUMTEXT NULL,
	`Lines` INT(20) NULL DEFAULT NULL,
	`FilePath` VARCHAR(255) NULL DEFAULT NULL,
	`header` MEDIUMTEXT NULL,
	`body` MEDIUMTEXT NULL,
	UNIQUE INDEX `id` (`id`),
	UNIQUE INDEX `MessageID` (`Message-ID`),
	INDEX `Message-ID` (`Message-ID`),
	INDEX `Date` (`Date`),
	FULLTEXT INDEX `References` (`References`),
	FULLTEXT INDEX `MgID` (`Message-ID`)
)
COLLATE='utf8_general_ci'
ENGINE=MyISAM
AUTO_INCREMENT=819574
;

 */