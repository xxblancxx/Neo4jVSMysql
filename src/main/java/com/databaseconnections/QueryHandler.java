/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.databaseconnections;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Random;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

/**
 *
 * @author martin
 */
public class QueryHandler {

    private static List<String> sqlqueries;
    private static List<String> neoqueries;
    private static List<Double> runtimesMysql;
    private static List<Double> runtimesNeo4j;

    public static void main(String[] args) {

        runtimesMysql = new ArrayList<Double>();
        runtimesNeo4j = new ArrayList<Double>();

        queryBuilderMysql();
        queryBuilderNeo4j();
        String currentQuery = "";

        Random rand = new Random();
        int[] randomPeople = new int[20];
        int value = 0;
        for (int i = 0; i < randomPeople.length; i++) {
            value = rand.nextInt(200);
            randomPeople[i] = value;
        }

        for (int i = 0; i < randomPeople.length; i++) {
            System.out.println("-------------------------------------------------");
            System.out.println("Person node_id: " + randomPeople[i]);
            System.out.println("<-----Mysql----->");
            for (int j = 0; j < sqlqueries.size(); j++) {
                currentQuery = sqlqueries.get(j).replace("PERSONID", String.valueOf(randomPeople[i]));
                double startTime = System.currentTimeMillis();
                depthMysql(currentQuery);
                double endTime = System.currentTimeMillis();
                runtimesMysql.add((endTime - startTime));
                System.out.println("Depth " + (j + 1) + " runtime: ms " + (endTime - startTime));
            }
            System.out.println("<-----Neo4j----->");
            for (int j = 0; j < neoqueries.size(); j++) {
                currentQuery = neoqueries.get(j).replace("PERSONID", String.valueOf(randomPeople[i]));
                double startTime = System.currentTimeMillis();
                depthNeo4j(currentQuery);
                double endTime = System.currentTimeMillis();
                runtimesNeo4j.add((endTime - startTime));
                System.out.println("Depth " + (j + 1) + " runtime: ms " + (endTime - startTime));
            }
        }
        getTimeComparisonValues();

    }

    public static void queryBuilderMysql() {
        sqlqueries = new ArrayList<String>();
        sqlqueries.add("SELECT Person.name, Person2.name \n"
                + "FROM Person, Person as Person2, Endorsement \n"
                + "WHERE Person.node_id = PERSONID\n"
                + "AND Person.node_id = Endorsement.source_node_id \n"
                + "AND Person2.node_id = Endorsement.target_node_id");

        sqlqueries.add("SELECT Person.name, Person2.name, Person3.name \n"
                + "FROM Person, Person as Person2,Person as Person3, Endorsement, Endorsement as Endorsement2\n"
                + "WHERE Person.node_id = PERSONID\n"
                + "AND Person.node_id = Endorsement.source_node_id \n"
                + "AND Person2.node_id = Endorsement.target_node_id\n"
                + "AND Person2.node_id = Endorsement2.source_node_id\n"
                + "AND Person3.node_id = Endorsement2.target_node_id");

        sqlqueries.add("SELECT Person.name, Person2.name, Person3.name, Person4.name\n"
                + "FROM Person, Person as Person2,Person as Person3,Person as Person4, Endorsement, Endorsement as Endorsement2,Endorsement as Endorsement3\n"
                + "WHERE\n"
                + "Person.node_id = PERSONID\n"
                + "AND Person.node_id = Endorsement.source_node_id \n"
                + "AND Person2.node_id = Endorsement.target_node_id\n"
                + "AND Person2.node_id = Endorsement2.source_node_id\n"
                + "AND Person3.node_id = Endorsement2.target_node_id \n"
                + "AND Person3.node_id = Endorsement3.source_node_id\n"
                + "AND Person4.node_id = Endorsement3.target_node_id ");

        sqlqueries.add("SELECT DISTINCT Person.name, Person2.name, Person3.name, Person4.name, Person5.name\n"
                + "FROM Person, Person as Person2,Person as Person3,Person as Person4,Person as Person5, Endorsement, Endorsement as Endorsement2,Endorsement as Endorsement3,Endorsement as Endorsement4\n"
                + "WHERE\n"
                + "Person.node_id = PERSONID\n"
                + "AND Person.node_id = Endorsement.source_node_id \n"
                + "AND Person2.node_id = Endorsement.target_node_id\n"
                + "AND Person2.node_id = Endorsement2.source_node_id\n"
                + "AND Person3.node_id = Endorsement2.target_node_id \n"
                + "AND Person3.node_id = Endorsement3.source_node_id\n"
                + "AND Person4.node_id = Endorsement3.target_node_id \n"
                + "AND Person4.node_id = Endorsement4.source_node_id\n"
                + "AND Person5.node_id = Endorsement4.target_node_id");

        sqlqueries.add("SELECT DISTINCT Person.name, Person2.name, Person3.name, Person4.name, Person5.name, Person6.name\n"
                + "FROM Person, Person as Person2,Person as Person3,Person as Person4,Person as Person5, Person as Person6, Endorsement, Endorsement as Endorsement2,Endorsement as Endorsement3,Endorsement as Endorsement4,Endorsement as Endorsement5\n"
                + "WHERE\n"
                + "Person.node_id = PERSONID\n"
                + "AND Person.node_id = Endorsement.source_node_id \n"
                + "AND Person2.node_id = Endorsement.target_node_id\n"
                + "AND Person2.node_id = Endorsement2.source_node_id\n"
                + "AND Person3.node_id = Endorsement2.target_node_id \n"
                + "AND Person3.node_id = Endorsement3.source_node_id\n"
                + "AND Person4.node_id = Endorsement3.target_node_id \n"
                + "AND Person4.node_id = Endorsement4.source_node_id\n"
                + "AND Person5.node_id = Endorsement4.target_node_id\n"
                + "AND Person5.node_id = Endorsement5.source_node_id\n"
                + "AND Person6.node_id = Endorsement5.target_node_id");
    }

    public static void queryBuilderNeo4j() {
        neoqueries = new ArrayList<String>();

        neoqueries.add("MATCH (p:Person)-[:ENDORSES]->(p2:Person) \n"
                + "WHERE p.id = PERSONID\n"
                + "RETURN p.name,p2.name");

        neoqueries.add("MATCH (p:Person)-[:ENDORSES]->(p2:Person)-[:ENDORSES]->(p3:Person) \n"
                + "WHERE p.id = PERSONID\n"
                + "RETURN p.name,p2.name,p3.name");

        neoqueries.add("MATCH(p:Person)-[:ENDORSES]->(p2:Person)-[:ENDORSES]->(p3:Person)-[:ENDORSES]->(p4:Person)\n"
                + "WHERE p.id=PERSONID\n"
                + "RETURN p.name,p2.name,p3.name,p4.name");

        neoqueries.add("MATCH(p:Person)-[:ENDORSES]->(p2:Person)-[:ENDORSES]->(p3:Person)-[:ENDORSES]->(p4:Person)-[:ENDORSES]->(p5:Person)\n"
                + "WHERE p.id=PERSONID\n"
                + "RETURN DISTINCT p.name,p2.name,p3.name,p4.name,p5.name");

        neoqueries.add("MATCH(p:Person)-[:ENDORSES]->(p2:Person)-[:ENDORSES]->(p3:Person)-[:ENDORSES]->(p4:Person)-[:ENDORSES]->(p5:Person)-[:ENDORSES]->(p6:Person)\n"
                + "WHERE p.id=PERSONID\n"
                + "RETURN DISTINCT p.name,p2.name,p3.name,p4.name,p5.name,p6.name");

    }

    public static void depthMysql(String query) {
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;

        String url = "jdbc:mysql://localhost:3306/db1?useSSL=false";
        String user = "root";
        String password = "pwd";

        try {

            con = DriverManager.getConnection(url, user, password);

            st = con.createStatement();

            rs = st.executeQuery(query);

        } catch (SQLException ex) {
            System.err.println(ex.getMessage());
            System.err.println(ex);
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (st != null) {
                    st.close();
                }
                if (con != null) {
                    con.close();
                }
            } catch (SQLException ex) {
                System.err.println(ex.getMessage());
                System.err.println(ex);
            }
        }
    }

    public static void depthNeo4j(String query) {

        Driver driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                AuthTokens.basic("neo4j", "class"));
        Session session = driver.session();

        // Run a query matching all nodes
        StatementResult result = session.run(query);

        session.close();
        driver.close();
    }

    public static void getTimeComparisonValues() {
        System.out.println("-------------------------Times-------------------------");
        System.out.println("-------------------------Mysql-------------------------");
        System.out.print("|");
        System.out.print("Min: " + Collections.min(runtimesMysql) + "ms |");
        System.out.print("Max: " + Collections.max(runtimesMysql) + "ms |");
        double average = runtimesMysql.stream().mapToDouble(a -> a).average().getAsDouble();
        System.out.print("Avg: " + average + "ms |");
        double median;
        Collections.sort(runtimesMysql);
        if (runtimesMysql.size() % 2 == 0) {
            median = ((double) runtimesMysql.get(runtimesMysql.size() / 2) + (double) runtimesMysql.get(runtimesMysql.size() / 2 - 1)) / 2;
        } else {
            median = (double) runtimesMysql.get(runtimesMysql.size() / 2);
        }
        System.out.print("Med: " + median + "ms |");
        System.out.println("\n-----------------------Neo4j-------------------------");
        System.out.print("|");
        System.out.print("Min: " + Collections.min(runtimesNeo4j) + "ms |");
        System.out.print("Max: " + Collections.max(runtimesNeo4j) + "ms |");
        average = runtimesNeo4j.stream().mapToDouble(a -> a).average().getAsDouble();
        System.out.print("Avg: " + average + "ms |");
        Collections.sort(runtimesNeo4j);
        if (runtimesNeo4j.size() % 2 == 0) {
            median = ((double) runtimesNeo4j.get(runtimesNeo4j.size() / 2) + (double) runtimesNeo4j.get(runtimesNeo4j.size() / 2 - 1)) / 2;
        } else {
            median = (double) runtimesNeo4j.get(runtimesNeo4j.size() / 2);
        }
        System.out.print("Med: " + median + "ms |");
        System.out.println("\n-------------------------------------------------------");
        System.out.println();
    }
}
