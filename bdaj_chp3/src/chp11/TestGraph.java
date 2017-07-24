package chp11;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.*;

public class TestGraph {
	public static void main(String[] args) {
		Graph graph = new DefaultGraph("SimpleGraph");
		graph.addNode("A" ).setAttribute("ui.label", "A");
		graph.addNode("B" ).setAttribute("ui.label", "B");;
		graph.addNode("C" ).setAttribute("ui.label", "C");;
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.display(true);
	}
	
}
