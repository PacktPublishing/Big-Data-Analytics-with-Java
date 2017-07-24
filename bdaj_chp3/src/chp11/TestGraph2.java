package chp11;
import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.*;

public class TestGraph2 {
	public static void main(String[] args) {
		Graph graph = new DefaultGraph("SimpleGraph");
		graph.addNode("John" ).setAttribute("ui.label", "A");
		graph.addNode("Ray" ).setAttribute("ui.label", "B");;
		graph.addNode("Terry" ).setAttribute("ui.label", "C");
		graph.addNode("Andy" ).setAttribute("ui.label", "C");
		graph.addNode("Mehr" ).setAttribute("ui.label", "C");
		graph.addNode("Preet" ).setAttribute("ui.label", "C");
		graph.addNode("Raj" ).setAttribute("ui.label", "C");
		graph.addNode("Viv" ).setAttribute("ui.label", "C");
		graph.addNode("Charlie" ).setAttribute("ui.label", "C");
		graph.addNode("James" ).setAttribute("ui.label", "C");
		
		graph.addEdge("JohnRay", "John", "Ray");
		graph.addEdge("John", "Terry", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");
		graph.addEdge("AB", "A", "B");
		graph.addEdge("BC", "B", "C");
		graph.addEdge("CA", "C", "A");		
		graph.display(true);
	}
	
}
