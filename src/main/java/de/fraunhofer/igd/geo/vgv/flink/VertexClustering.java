package de.fraunhofer.igd.geo.vgv.flink;

import java.util.List;
import java.util.ArrayList;
import java.lang.Character;
import java.lang.Math;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.Iterator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
public class VertexClustering {
    public static void main(String... args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String inputFile; // path to the input *.obj file
        if (params.has("in")) {
            inputFile = params.get("in");
        } else {
            throw new RuntimeException("Please provide an input file (--in)");
        }

        String outputFile; // path to the output *.obj file
        if (params.has("out")) {
            outputFile = params.get("out");
        } else {
            throw new RuntimeException("Please provide an input file (--out)");
        }

        float cellSize;
        if (params.has("cellSize")) {
            cellSize = Float.parseFloat(params.get("cellSize"));
        } else {
            throw new RuntimeException("Please provide a cell size (--cellSize)");
        }

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Zeilen als Strings lesen und vertex Zeilen in einem Tuple2<Character, Double[]> mit einem 'v' markieren, face Zeilen mit einem 'f', Kommentare mit einem 'c' und leere Zeilen mit einem 'e'. Daten werden als Double[3] gespeichert. setParallelism(1), um die Reihenfolge der vertices zu behalten, damit die Indizes spaeter stimmen.
	DataSet<Tuple2<Character, Double[]>> object = env.readTextFile(inputFile).setParallelism(1)
		.map(new MapFunction<String,Tuple2<Character,Double[]>>(){
			@Override
			public Tuple2<Character, Double[]> map(String line) throws Exception {
				if(line.length() > 0){
					char linetype = line.charAt(0); 
					if(linetype == 'v' || linetype == 'f') {
						String[] split = line.split(" ");
						Double[] values = {Double.valueOf(split[1]),Double.valueOf(split[2]),Double.valueOf(split[3])};
						return new Tuple2<>(linetype,values);
					}
					else {
						return new Tuple2<>('c',new Double[] {0.0,0.0,0.0});
					}
				}
				else {
					return new Tuple2<>('e', new Double[] {0.0,0.0,0.0});
				}
			}
		}).setParallelism(1);
	// Vertex Zeilen mit Indizes versehen und face Zeilen mit einer 0 markieren
	DataSet<Tuple2<Integer, Double[]>> verticesAndFaces = object
		.reduceGroup(new GroupReduceFunction<Tuple2<Character, Double[]>, Tuple2<Integer, Double[]>>() {
			@Override
			public void reduce(Iterable<Tuple2<Character, Double[]>> inline, Collector<Tuple2<Integer, Double[]>> outline) throws Exception {
				int i = 1;
				for(Tuple2<Character, Double[]> in : inline) {
					if(in.f0 == 'v') {
						outline.collect(new Tuple2<>(i,in.f1));
						i++;
					}
					else if(in.f0 == 'f') {
						outline.collect(new Tuple2<>(0,in.f1));
					}
				}
			}
		});
	// Vertex Zeilen filtern und in eigenem DataSet abspeichern
	DataSet<Tuple2<Integer, Double[]>> vertices = verticesAndFaces
		.filter(new FilterFunction<Tuple2<Integer, Double[]>>() {
			@Override
			public boolean filter(Tuple2<Integer, Double[]> data) {
				return data.f0 > 0;
			} 
		});
	// Face Zeilen filtern und in eigenem DataSet abspeichern. Die Tuple Elemente beinhalten nun die drei Indizes.
	DataSet<Tuple3<Integer, Integer, Integer>> faces = verticesAndFaces
		.filter(new FilterFunction<Tuple2<Integer, Double[]>>() {
			@Override
			public boolean filter(Tuple2<Integer, Double[]> data) {
				return data.f0 == 0;
			} 
		})
		.map(new MapFunction<Tuple2<Integer,Double[]>, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> map(Tuple2<Integer, Double[]> face) throws Exception {
				return new Tuple3<>(face.f1[0].intValue(), face.f1[1].intValue(), face.f1[2].intValue());
			}
		});
	// Einteilung der Vertices in Zellen mithilfe 3D Morton Code
	Configuration config = new Configuration();
	config.setFloat("cellSize",cellSize);
	DataSet<Tuple3<Double[], Integer, Integer>> vertex_medians = vertices
		.map(new RichMapFunction<Tuple2<Integer, Double[]>, Tuple3<Long, Double[], Integer>>() {
			private Float cellSize;
			public void open(Configuration parameters) throws Exception {
				cellSize = parameters.getFloat("cellSize",0);
			}
			@Override 
			public Tuple3<Long, Double[], Integer> map(Tuple2<Integer, Double[]> vertex) throws Exception {
				Tuple3<Long, Double[], Integer> assignedVertex = new Tuple3<>(0L,vertex.f1,vertex.f0) ;               		
				long xID = (long) (vertex.f1[0]/cellSize);
                		xID = zeroInterleaveBits(0xffff&xID);
                		long yID = (long) (vertex.f1[1]/cellSize);
                		yID = zeroInterleaveBits(0xffff&yID);
                		long zID = (long) (vertex.f1[2]/cellSize);
				zID = zeroInterleaveBits(0xffff&zID);
				assignedVertex.f0 = xID | (yID << 1) | (zID << 2);
                		return assignedVertex;
			}
		}).withParameters(config)
		.groupBy(0)
	// Berechnung der euklidischen Distanzen fuer jede Zelle und des Medians. Alte Vertex Koordinaten werden mit dem Median ueberschrieben und mit ihren alten Indizes und der 3D Morton Gruppen ID abgespeichert
		.reduceGroup(new GroupReduceFunction<Tuple3<Long, Double[], Integer>, Tuple3<Double[], Integer, Long>>(){
			@Override
			public void reduce(Iterable<Tuple3<Long, Double[], Integer>> vertex, Collector<Tuple3<Double[], Integer, Long>> medianVertex) throws Exception {
				double[] mean = {0.0,0.0,0.0};
				List<Double[]> vertices = new ArrayList<Double[]>();
				List<Integer> indices = new ArrayList<Integer>(); 
				Long group = 0L;
				for(Tuple3<Long, Double[], Integer> v : vertex) {
					for(int i = 0; i < 3; i++){
						mean[i] += v.f1[i];
					}
					indices.add(v.f2);
					vertices.add(v.f1);
					group = v.f0;
				}
				int n_verts = vertices.size();
				for(int i = 0; i < 3; i++){
					mean[i] /= n_verts;
				}	
				Double[] vert = new Double[3];
				double euclidean_dist = 1.0;
				double euclidean_minimum = 1.0;
				int ind_Median = 0;
				for(int i = 0; i < n_verts; i++) {
					vert = vertices.get(i);
					euclidean_dist = Math.sqrt(Math.pow(vert[0],2) + Math.pow(vert[1],2) + Math.pow(vert[2],2));
					if(euclidean_dist < euclidean_minimum){
						euclidean_minimum = euclidean_dist;
						ind_Median = i;
					}
				}
				Iterator<Integer> index = indices.iterator();
				while(index.hasNext()){
					medianVertex.collect(new Tuple3<>(vertices.get(ind_Median), index.next(), group));	
				}
			}
		})
	// Erzeugung neuer Indizes fuer die in der vorigen GroupReduceFunction berechneten Mediane. 
		.reduceGroup(new GroupReduceFunction<Tuple3<Double[], Integer, Long>, Tuple3<Double[], Integer, Integer>>() {
			@Override
			public void reduce(Iterable<Tuple3<Double[], Integer, Long>> medianVertices, Collector<Tuple3<Double[], Integer, Integer>> newIndVertices) throws Exception {
				Integer newInd = 0;
				Long group = 0L;
				for(Tuple3<Double[], Integer, Long> mvs : medianVertices) {
					if(group - mvs.f2 != 0) {
						newInd++;
					}
					group = mvs.f2;
					newIndVertices.collect(new Tuple3<>(mvs.f0, mvs.f1, newInd));
				}
			}
		}).setParallelism(1); 
	// Austauschen der alten Indizes im DataSet<...> faces durch die in der vorigen GroupReduceFunction erzeugten neuen Indizes
	faces = faces
		.join(vertex_medians)
		.where("f0")
		.equalTo("f1")
		.with(new JoinFunction<Tuple3<Integer, Integer, Integer>,Tuple3<Double[], Integer, Integer>, Tuple3<Integer, Integer, Integer>>(){
			@Override
			public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> face, Tuple3<Double[], Integer, Integer> vertex) throws Exception {
				return new Tuple3<>(vertex.f2, face.f1, face.f2);
			}
		})
		.join(vertex_medians)
		.where("f1")
		.equalTo("f1")
		.with(new JoinFunction<Tuple3<Integer, Integer, Integer>,Tuple3<Double[], Integer, Integer>, Tuple3<Integer, Integer, Integer>>(){
			@Override
			public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> face, Tuple3<Double[], Integer, Integer> vertex) throws Exception {
				return new Tuple3<>(face.f0, vertex.f2, face.f2);
			}
		})
		.join(vertex_medians)
		.where("f2")
		.equalTo("f1")
		.with(new JoinFunction<Tuple3<Integer, Integer, Integer>,Tuple3<Double[], Integer, Integer>, Tuple3<Integer, Integer, Integer>>(){
			@Override
			public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> face, Tuple3<Double[], Integer, Integer> vertex) throws Exception {
				return new Tuple3<>(face.f0, face.f1, vertex.f2);
			}
		});
	// Zusammenstellung der Strings der vertex Zeilen der Ausgabedatei
	DataSet<String> vertex_Strings = vertex_medians
		.groupBy(2)
		.reduceGroup(new GroupReduceFunction<Tuple3<Double[], Integer, Integer>, String> () {
			@Override
			public void reduce(Iterable<Tuple3<Double[], Integer, Integer>> vertices, Collector<String> vlines) throws Exception {
				Double[] v = vertices.iterator().next().f0;
				vlines.collect("v " + v[0] + " " + v[1] + " " + v[2]);
				
			}
		}).setParallelism(1);
	// Zusammenstellung der Strings der face Zeilen der Ausgabedatei
	DataSet<String> face_Strings = faces
		.reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, String> () {
			@Override
			public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> faces, Collector<String> flines) throws Exception {
				for(Tuple3<Integer, Integer, Integer> f : faces) {
					flines.collect("f " + f.f0 + " " + f.f1 + " " + f.f2);
				}
			}
		}).setParallelism(1);
	// Zusammenfuegen der vertex Zeilen und der face Zeilen und Abspeichern in Ausgabedatei
	vertex_Strings
	.union(face_Strings.distinct())
	.writeAsText(outputFile,FileSystem.WriteMode.OVERWRITE).setParallelism(1);	
	env.execute();
	System.out.println("Done");
    }
	// 2-Zero-Interleave Funktion. Beispiel: Aus abcd wird a00b00c00d 
        private static final long bitmasks[] = {0x249249249249L, 0xc30c30c30c3L, 0xf00f00f00fL, 0xfff00fffL};
        private static final long bitshifts[] = {0x2, 0x4, 0x8, 0x10};
        public static long zeroInterleaveBits(long bits){
            for(int i = 3; i >= 0; i--){
                    bits = (bits | (bits << bitshifts[i])) & bitmasks[i];
            }
            return bits;
        }
}
