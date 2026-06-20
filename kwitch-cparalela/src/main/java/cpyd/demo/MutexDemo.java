package cpyd.demo;

import cpyd.distributed.LamportClock;
import cpyd.distributed.NodeLogger;
import cpyd.distributed.NodeMembership;
import cpyd.distributed.RicartAgrawala;

import java.util.List;

/*
Demo de exclusion mutua distribuida (Ricart-Agrawala).

Levanta dos nodos (Nodo_A y Nodo_B) en una sola JVM, cada uno con su propio
reloj de Lamport, y los hace pedir la seccion critica al mismo tiempo para
ver como el algoritmo resuelve la competencia:

  - Ambos pasan a DESEADO y se mandan REQUEST mutuamente.
  - Los timestamps empatan, asi que gana el de menor id (Nodo_A).
  - Nodo_B le responde REPLY a Nodo_A; Nodo_A difiere el REQUEST de Nodo_B.
  - Nodo_A entra a la seccion critica; Nodo_B espera.
  - Al salir, Nodo_A manda el REPLY diferido y recien ahi entra Nodo_B.

Nunca estan los dos dentro de la seccion critica a la vez.

Ejecutar:  java -cp target/classes cpyd.demo.MutexDemo
*/
public class MutexDemo {

    private static final int PORT_A = 9101;
    private static final int PORT_B = 9102;

    public static void main(String[] args) throws InterruptedException {
        NodeLogger logger = new NodeLogger("MutexDemo");

        //membresia compartida: ambos nodos estan vivos
        NodeMembership membership = new NodeMembership();
        membership.registerNode("Nodo_A", "localhost", PORT_A);
        membership.registerNode("Nodo_B", "localhost", PORT_B);

        //cada nodo tiene su propio reloj logico (no hay reloj global)
        LamportClock clockA = new LamportClock();
        LamportClock clockB = new LamportClock();

        RicartAgrawala raA = new RicartAgrawala("Nodo_A", PORT_A,
            List.of(new RicartAgrawala.PeerInfo("Nodo_B", "localhost", PORT_B)),
            membership, clockA, logger, null);
        RicartAgrawala raB = new RicartAgrawala("Nodo_B", PORT_B,
            List.of(new RicartAgrawala.PeerInfo("Nodo_A", "localhost", PORT_A)),
            membership, clockB, logger, null);

        Thread.sleep(500); //esperar a que ambos receivers esten escuchando

        logger.log("===== DEMO EXCLUSION MUTUA: A y B piden la CS a la vez =====");

        Thread tA = new Thread(() -> seccionCritica(raA, logger, "Nodo_A"), "job-A");
        Thread tB = new Thread(() -> seccionCritica(raB, logger, "Nodo_B"), "job-B");

        tA.start();
        tB.start();
        tA.join();
        tB.join();

        logger.log("===== FIN: la exclusion mutua impidio el solapamiento en la CS =====");
        System.exit(0);
    }

    private static void seccionCritica(RicartAgrawala ra, NodeLogger logger, String quien) {
        ra.requestCS();
        logger.log(">>> " + quien + " DENTRO de la seccion critica (recurso exclusivo)");
        try {
            Thread.sleep(1500); //simula trabajo dentro de la CS
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.log("<<< " + quien + " saliendo de la seccion critica");
        ra.releaseCS();
    }
}
