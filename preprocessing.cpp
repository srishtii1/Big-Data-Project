#include <vector>
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>

using namespace std;

void read_csv(){
    fstream fin;
    fin.open("SingaporeWeather/SingaporeWeather.csv", ios::in);
    vector<string> row;
    string line, word, temp;
    int counter = 0;

    while (fin >> temp) {
        row.clear();
        getline(fin, line);
        stringstream s(line);
        while (getline(s, word, ',')) {
            row.push_back(word);
        }
        counter++;
        if (counter % 10000 == 0){
            for (string ele: row) {
                cout << ele << " ";
            }
            cout << endl;
        }
    }
    
    return;
}

int main() {
    read_csv();
    return 0;
}